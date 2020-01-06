(declare-project
  :name "pgjobq"
  :author "Andrew Chambers")

(declare-source
  :source ["pgjobq.janet"])

(declare-binscript
  :main "pgjobq-worker")