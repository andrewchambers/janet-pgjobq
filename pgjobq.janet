(import json)
(import pq)
(import redis)

(def job-schema `
  create table jobq(jobid bigserial primary key, q TEXT, data jsonb, result jsonb, completedat timestamptz);

  create index jobqqindex on jobq(q);

  create index jobqcompletedatindex on jobq(completedat);
`)

(defn count-pending-jobs
  [pg-conn qname]
  ((pq/one pg-conn "select count(*)::integer from jobq where q = $1 and completedat is null;" qname) "count"))

(defn try-enqueue-job 
  [pg-conn qname job-data &opt limit]
  (if (or (nil? limit) (< (count-pending-jobs pg-conn qname) limit))
    ((pq/one pg-conn "insert into jobq(q, data) values($1, $2) returning jobid;" qname (pq/jsonb job-data)) "jobid")
    nil))

(defn notify-job-worker
  [redis-conn qname]
  (redis/command redis-conn "publish" (string "pgjobq/" qname "-notify") "notify"))

(defn publish-job-result [pg-conn redis-conn jobid result]
  (pq/exec pg-conn "update jobq set result = $1, completedat = current_timestamp where jobid = $2;" (pq/jsonb result) jobid)
  (redis/command redis-conn "publish" (string "pgjobq/job-" jobid) (json/encode result)))

(defn query-job [pg-conn jobid] 
  (def j (pq/one pg-conn "select jobid, data, result from jobq where jobid = $1;" jobid))
  (when (nil? j) (error (string "no job with id " jobid)))
  j)

(defn query-job-result [pg-conn jobid] 
  (def j (pq/one pg-conn "select result from jobq where jobid = $1;" jobid))
  (when (nil? j) (error (string "no job with id " jobid)))
  (get j "result"))

(defn- redis-subscribe
  [redis-conn topic]
  (def reply (redis/command redis-conn "subscribe" topic))
  (unless (= (reply 0) "subscribe") (error "unexpected response from server")))

(defn subscribe-redis-conn-to-job-completion
  [redis-conn jobid]
  (redis-subscribe redis-conn (string "pgjobq/job-" jobid)))

(defn wait-for-job-completion
  [pg-conn dial-redis jobid & timeout]
  (with [redis-conn (dial-redis)]
    (when timeout
      (redis/set-timeout timeout))
    (subscribe-redis-conn-to-job-completion redis-conn jobid)
    (if-let [r (query-job-result pg-conn jobid)]
      r
      (do 
        (def reply (redis/get-reply redis-conn))
        (when (not= (get reply 0) "message") (error "unexpected redis reply"))
        (def result (json/decode (reply 2)))
        result))))

(defn next-job
  [pg-conn qname] 
  (pq/one pg-conn "select * from jobq where q = $1 and completedat is null order by jobid asc limit 1;" qname))

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
        (def result (run-job (j "data")))
        (with [redis-conn (dial-redis)]
          (publish-job-result pg-conn redis-conn (j "jobid") result))))))

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