(import jdn)
(import pq)
(import redis)

(def job-schema `
  create table jobq(jobid bigserial primary key, position bigserial, q TEXT, data TEXT, result TEXT, completedat timestamptz);

  create index jobqqindex on jobq(q);

  create index jobqpositionindex on jobq(q);

  create index jobqcompletedatindex on jobq(completedat);
`)

(defn count-pending-jobs
  [pg-conn qname]
  (pq/val pg-conn "select count(*)::integer from jobq where q = $1 and completedat is null;" qname))

(defn try-enqueue-job
  [pg-conn qname job-data &opt limit]
  (if (or (nil? limit) (< (count-pending-jobs pg-conn qname) limit))
    (pq/val pg-conn "insert into jobq(q, data) values($1, $2) returning jobid;" qname (jdn/encode job-data))
    nil))

(defn reschedule-job
  [pg-conn jobid]
  (pq/exec pg-conn
           "update jobq set position = nextval('jobq_position_seq') where jobid = $1;"
           jobid))

(defn notify-job-worker
  [redis-conn qname]
  (redis/command redis-conn "publish" (string "pgjobq/" qname "-notify") "notify"))

(defn publish-job-result [pg-conn redis-conn jobid result]
  (pq/exec
    pg-conn
    "update jobq set result = $1, completedat = current_timestamp where jobid = $2 and completedat is null;"
    (jdn/encode result) jobid)
  (redis/command redis-conn "publish" (string "pgjobq/job-" jobid) (jdn/encode result)))

(defn query-job [pg-conn jobid]
  (when-let [j (pq/row pg-conn "select jobid, data, result from jobq where jobid = $1;" jobid)]
    (put j :data (jdn/decode (j :data)))
    (when (j :result)
      (put j :result (jdn/decode (j :result))))
    j))

(defn query-job-result [pg-conn jobid]
  (when-let [j (query-job pg-conn jobid)]
    (j :result)))

(defn- redis-subscribe
  [redis-conn topic]
  (def reply (redis/command redis-conn "subscribe" topic))
  (unless (= (reply 0) "subscribe") (error "unexpected response from server")))

(defn subscribe-redis-conn-to-job-completion
  [redis-conn jobid]
  (redis-subscribe redis-conn (string "pgjobq/job-" jobid)))

(defn wait-for-job-completion
  [pg-conn redis-conn jobid & timeout]

  (default timeout 10)
  (def orig-redis-conn-timeout (redis/get-timeout redis-conn))

  (defn restore-redis-conn
    [c]
    # Reconnect is the easiest way to ensure the connection
    # is not in a wonky state (like subscription active)
    # when an error has occured.
    (redis/reconnect c)
    (redis/set-timeout c ;orig-redis-conn-timeout))

  (with [redis-conn restore-redis-conn]
    (redis/set-timeout timeout 0)
    (subscribe-redis-conn-to-job-completion redis-conn jobid)
    (if-let [r (query-job-result pg-conn jobid)]
      r
      (match (redis/get-reply redis-conn)
        ["message" _ j]
        (jdn/decode j)
        (error "unexpected redis reply")))))

(defn next-job
  [pg-conn qname]
  (def j (pq/row pg-conn "select * from jobq where (q = $1 and completedat is null) order by position asc limit 1;" qname))
  (when j
    (put j :data (jdn/decode (j :data))))
  j)

(defn run-worker
  [dial-pq dial-redis qname run-job &keys {:fallback-poll-timer fallback-poll-timer}]
  (default fallback-poll-timer 120)
  (with [pg-conn (dial-pq)]
    (while true
      (def j
        (if-let [j (next-job pg-conn qname)]
          j
          (with [redis-conn (dial-redis)]
            (redis/set-timeout redis-conn fallback-poll-timer)
            (redis-subscribe redis-conn (string "pgjobq/" qname "-notify"))
            (if-let [j (next-job pg-conn qname)]
              j
              (do
                # If our timeout triggers, or redis disconnects
                # see if we have a job anyway. otherwise we wait again.
                (protect (redis/get-reply redis-conn))
                (next-job pg-conn qname))))))
      (when (not (nil? j))
        (match (run-job pg-conn (j :data))
          [:job-complete result]
          (with [redis-conn (dial-redis)]
            (publish-job-result pg-conn redis-conn (j :jobid) result))
          :reschedule
          (reschedule-job pg-conn (j :jobid))
          v
          (errorf "job worker returned an unexpected result %p" v))))))

# Repl test helpers.
# (import ./pgjobq :as pgjobq)
# (def pg-conn (pq/connect "postgresql://localhost?dbname=postgres"))
# (def redis-conn (redis/connect "localhost"))
# (pq/exec pg-conn "BEGIN TRANSACTION;")
#   (each stmt (string/split "\n\n" pgjobq/job-schema)
#     (pq/exec pg-conn stmt))
# (pq/exec pg-conn "COMMIT;")
# (pgjobq/try-enqueue-job pg-conn "testq" @{"hello" "world"})
# (pgjobq/notify-job-worker redis-conn "testq")

