(ns hyperion.redis-spec
  (:require [speclj.core :refer :all]
            [hyperion.api :refer :all]
            [hyperion.log :as log]
            [accession.core :as redis]
            [hyperion.dev.spec :refer [it-behaves-like-a-datastore]]
            [hyperion.redis.spec-helper :refer [with-testable-redis-datastore]]
            [hyperion.redis :refer :all]))

(hyperion.log/error!)

(describe "Redis Datastore"

  (context "Creating a redis datastore"
    (it "from options"
      (let [ds (new-redis-datastore :host "localhost" :port 6379)
            db (.db ds)]
        (should= "PONG" (redis/with-connection db (redis/ping))))))

    (it "using factory"
      (let [ds (new-datastore :implementation :redis :host "localhost" :port 6379)
            db (.db ds)]
        (should= "PONG" (redis/with-connection db (redis/ping)))))

  (context "Live"

    (with-testable-redis-datastore)

    (it-behaves-like-a-datastore)))
