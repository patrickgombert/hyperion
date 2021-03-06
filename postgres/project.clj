(defproject hyperion/hyperion-postgres "3.7.0"
  :description "Postgres Datastore for Hyperion"
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [hyperion/hyperion-api "3.7.0"]
                 [hyperion/hyperion-sql "3.7.0"]
                 [postgresql/postgresql "8.4-702.jdbc4"]]

  ; leiningen 1
  :dev-dependencies [[speclj "2.5.0"]]
  :test-path "spec"

  ; leiningen 2
  :profiles {:dev {:dependencies [[speclj "2.5.0"]]}}
  :test-paths ["spec/"]
  :plugins [[speclj "2.5.0"]])
