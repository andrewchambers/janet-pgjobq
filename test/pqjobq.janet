(import json)
(import sh)
(import process)
(import redis)
(import pq)
(import ../pgjobq :as pgjobq)

(defmacro assert
  [cond]
  ~(unless ,cond (error "fail")))

(defn spawn-temp-databases
  []
  (def d (string (sh/$$_ ["mktemp" "-d" "/tmp/pgjobq-test.tmp.XXXXXX"])))
  (def pg-dir (string d "/" "pg-dir"))
  (def redis-dir (string d "/" "redis-dir"))
  (unless (os/mkdir pg-dir) (error "unable to make pg-dir"))
  (unless (os/mkdir redis-dir) (error "unable to make redis-dir"))
  
  (def redis-port 5686)
  (def r (process/spawn 
     ["redis-server"
      # We can't use unix sockets because the SIGPIPE aborts our tests.
      "--port" (string redis-port)]
     :redirects [[stderr :discard] [stdout :discard]]
     :start-dir redis-dir))

  (sh/$ ["pg_ctl" "-s" "-D" pg-dir  "initdb" "-o" "--auth=trust"])
  (sh/$ ["pg_ctl" "-s" "-w" "-D" pg-dir   "start" "-l" (string pg-dir  "/test-log-file.txt")])

  (def cleanup-script (string d "/" "cleanup.sh"))
  (spit cleanup-script 
    (string `
set -u

cleanup () {
  echo "cleaning up temporary databases..."
  set -x
  redis-cli  -p ` (string redis-port) `  shutdown
  pg_ctl -s -w -D ` pg-dir ` stop -m immediate
  rm -rf ` (sh/shell-quote [d]) `

  exit 0
}

trap cleanup EXIT
while true
do
  sleep 1
done
`
    ))
  (def cleanup-proc (process/spawn ["sh" cleanup-script]))
  (os/sleep 0.2)
  @{
    :connect-pg (fn [&] (pq/connect "postgresql://localhost?dbname=postgres"))
    :connect-redis (fn [&] (redis/connect "localhost" redis-port))
    :r r
    :dir d
    :cleanup-proc cleanup-proc
    :close
      (fn [&] (:close cleanup-proc))})

(defn run-tests
  []
  (with [tmp-dbs (spawn-temp-databases)]

    (def pg-conn (:connect-pg tmp-dbs))
    (def redis-conn (:connect-redis tmp-dbs))
    
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
                     @{"jobid" (int/s64 1) "q" "testq" "data" @{"some" "job"}}))
      (pgjobq/publish-job-result pg-conn redis-conn (int/s64 1) @{"status" "done"})
      (assert (deep= (pgjobq/next-job pg-conn "testq")
                     @{"jobid" (int/s64 2) "q" "testq" "data" @{"some" "other-job"}})))
      (with [sub-conn (:connect-redis tmp-dbs)]
        (pgjobq/subscribe-redis-conn-to-job-completion sub-conn (int/s64 2))
        (pgjobq/publish-job-result pg-conn redis-conn (int/s64 2) @{"status" "done"})
        (def r (redis/get-reply sub-conn))
        (put r 2 (json/decode (r 2)))
        (assert (deep= @["message" "pgjobq/job-2" @{"status" "done"}] r))
        (def j (pgjobq/query-job pg-conn (int/s64 2)))
        (assert (deep= (j "result") @{"status" "done"})))))

(run-tests)