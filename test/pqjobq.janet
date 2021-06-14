(import jdn)
(import sh)
(import posix-spawn)
(import redis)
(import pq)
(import shlex)
(import tmppg)
(import ../pgjobq)

(defn tmpredis
  []
  (def port 35543)

  (def d (sh/$<_ mktemp -d /tmp/janet-redis-test.tmp.XXXXX))

  (def r (posix-spawn/spawn ["sh" "-c"
                             (string
                               "cd " (shlex/quote d) " ;"
                               "exec redis-server --port " port " > /dev/null 2>&1")]))
  (os/sleep 0.5)

  @{:port port
    :d d
    :r r
    :connect
    (fn [self]
      (redis/connect "localhost" (self :port)))
    :close
    (fn [self]
      (print "closing down server...")
      (:close (self :r))
      (sh/$ rm -rf (self :d)))})

(defn run-tests
  []
  (with [tmp-redis (tmpredis)]
    (with [tmp-pg (tmppg/tmppg)]

      (def pg-conn (pq/connect (tmp-pg :connect-string)))
      (def redis-conn (:connect tmp-redis))

      # Setup the job tables
      (pq/exec pg-conn "BEGIN TRANSACTION;")
      (each stmt (string/split "\n\n" pgjobq/job-schema)
        (pq/exec pg-conn stmt))
      (pq/exec pg-conn "COMMIT;")

      # basic sanity.
      (do
        (assert (nil? (pgjobq/next-job pg-conn "testq")))
        (assert (pgjobq/try-enqueue-job pg-conn "testq" @{"some" "job"}))
        (assert (pgjobq/try-enqueue-job pg-conn "testq" @{"some" "other-job"}))
        (assert (nil? (pgjobq/try-enqueue-job pg-conn "testq" @{"some" "too many jobs"} 2)))
        (assert (= (pgjobq/count-pending-jobs pg-conn "testq") 2))
        (assert (deep= (pgjobq/next-job pg-conn "testq")
                       @{:jobid (int/s64 1) :position (int/s64 1) :q "testq" :data @{"some" "job"}}))
        (pgjobq/publish-job-result pg-conn redis-conn (int/s64 1) @{"status" "done"})
        (pgjobq/reschedule-job pg-conn 2)
        (assert (deep= (pgjobq/next-job pg-conn "testq")
                       @{:jobid (int/s64 2) :position (int/s64 3) :q "testq" :data @{"some" "other-job"}})))
      (with [sub-conn (:connect tmp-redis)]
        (pgjobq/subscribe-redis-conn-to-job-completion sub-conn (int/s64 2))
        (pgjobq/publish-job-result pg-conn redis-conn (int/s64 2) @{:status "done"})
        (def r (redis/get-reply sub-conn))
        (put r 2 (jdn/decode (r 2)))
        (assert (deep= @["message" "pgjobq/job-2" @{:status "done"}] r))
        (def j (pgjobq/query-job pg-conn (int/s64 2)))
        (assert (deep= (j :result) @{:status "done"}))))))

(run-tests)
