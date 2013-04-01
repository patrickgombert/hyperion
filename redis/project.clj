(defproject hyperion/hyperion-redis "3.7.0"
  :description "Redis Datastore for Hyperion"
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [hyperion/hyperion-api "3.7.0"]
                 [org.clojure/data.json "0.2.1"]
                 [accession "0.1.1" :exclusions [org.clojure/clojure]]]

  ; leiningen 1
  :dev-dependencies [[speclj "2.5.0"]]
  :test-path "spec"

  ; leiningen 2
  :profiles {:dev {:dependencies [[speclj "2.5.0"]]}}
  :test-paths ["spec/"]
  :plugins [[speclj "2.5.0"]])
