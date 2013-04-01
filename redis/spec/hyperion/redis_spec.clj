(ns hyperion.redis-spec
  (:require [speclj.core :refer :all]
            [hyperion.api :refer :all]
            [hyperion.log :as log]
            [accession.core :as redis]
            [hyperion.dev.spec :refer [it-behaves-like-a-datastore]]
            [hyperion.redis.spec-helper :refer [with-testable-redis-datastore]]
            [hyperion.redis :refer :all]))

(hyperion.log/error!)

(defentity :types
  [bool]
  [bite :type java.lang.Byte]
  [shrt :type java.lang.Short]
  [inti]
  [lng :type java.lang.Long]
  [flt :type java.lang.Float]
  [dbl]
  [str]
  [chr :type java.lang.Character]
  [kwd :type clojure.lang.Keyword])

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

  (context "all-kinds"
    (with-testable-redis-datastore)

    (it "with no records"
      (should (empty? (.ds-all-kinds *ds*))))

    (it "with one kind"
      (.ds-save *ds* [{:kind "testing"}])
      (should-not (empty? (.ds-all-kinds *ds*))))

    (it "with multiple kinds"
      (.ds-save *ds* [{:kind "testing1"} {:kind "testing2"} {:kind "testing3"}])
      (let [found-kinds (.ds-all-kinds *ds*)]
        (should= 3 (count found-kinds))
        (should (some #(= "testing1" %) found-kinds))
        (should (some #(= "testing2" %) found-kinds))
        (should (some #(= "testing3" %) found-kinds)))))

  (context "Live"
    (with-testable-redis-datastore)

    (it-behaves-like-a-datastore)))
