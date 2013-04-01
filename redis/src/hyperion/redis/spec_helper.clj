(ns hyperion.redis.spec-helper
  (:require [speclj.core :refer :all]
            [hyperion.redis :refer :all]
            [accession.core :as redis]
            [hyperion.api :refer [*ds*]]))

(defn- clear-db [db]
  (let [test-keys (redis/with-connection db (redis/keys "*:hyperion"))]
    (when (seq test-keys)
      (redis/with-connection db (apply redis/del test-keys)))))

(defn with-testable-redis-datastore []
  (list
    (around [it]
      (let [ds (new-redis-datastore :host "127.0.0.1" :port 6379)]
        (binding [*ds* ds]
          (try
            (it)
            (finally
              (clear-db (.db ds)))))))))
