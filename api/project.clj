(defproject hyperion/hyperion-api "3.7.0"
  :description "A generic persistence API for Clojure"
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [chee "1.0.0"]
                 [org.clojure/data.codec "0.1.0"]
                 [com.taoensso/timbre "1.0.0"]]

  ; leiningen 1
  :dev-dependencies [[speclj "2.5.0"]]
  :test-path "spec"

  ; leiningen 2
  :profiles {:dev {:dependencies [[speclj "2.5.0"]]}}
  :test-paths ["spec/"]
  :plugins [[speclj "2.5.0"]
            [lein-autodoc "0.9.0"]])
