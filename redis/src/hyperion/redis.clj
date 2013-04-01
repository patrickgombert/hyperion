(ns hyperion.redis
  (:require [hyperion.abstr :refer [Datastore]]
            [hyperion.api :refer :all]
            [hyperion.key :refer [generate-id]]
            [hyperion.memory :as memory]
            [hyperion.filtering :as filter]
            [hyperion.sorting :as sort]
            [hyperion.redis.meta-record :refer [meta-record meta-key cast-record]]
            [accession.core :as redis]
            [chee.util :refer [->options]]))

(defn- hyperionize-key [key] (str key ":hyperion"))
(def all-kinds-key (hyperionize-key "__hyperion_kinds__"))

(defn- list->map [list]
  (apply hash-map list))

(defn- map->keyword-map [map]
  (reduce (fn [acc raw-pair] (assoc acc (keyword (first raw-pair)) (second raw-pair))) {} map))

(defn- db-specification [options]
  (let [sanitized-options (select-keys options [:host :port :password :socket :timeout])]
    (redis/connection-map sanitized-options)))

(defn- save-record [db record]
  (let [record-key (:key record)
        stringified-record (zipmap (map name (keys record)) (vals record))
        meta-record (meta-record stringified-record)]
    (redis/with-connection db
      (apply redis/hmset record-key (flatten (seq stringified-record)))
      (apply redis/hmset (meta-key record-key) (flatten (seq meta-record)))
      (redis/sadd all-kinds-key (:kind record)))
    record))

(defn- create [db record]
  (let [kind (:kind record)
        create-key (str (name kind) ":" (generate-id))
        record-key (hyperionize-key create-key)]
    (save-record db (assoc record :key record-key))))

(defn- save-records [db records]
  (let [create-records (filter #(nil? (:key %)) records)
        update-records (filter :key records)]
    (into []
      (concat
        (map (partial create db) create-records)
        (map (partial save-record db) update-records)))))

(defn- delete-record-by-key [db record-key]
  (redis/with-connection db
    (redis/del record-key)
    (redis/del (meta-key record-key)))
  record-key)

(defn- find-record-by-key [db record-key]
  (let [record (map->keyword-map (list->map (redis/with-connection db (redis/hgetall record-key))))]
    (if (empty? record)
      nil
      (cast-record record
        (map->keyword-map (list->map (redis/with-connection db (redis/hgetall (meta-key (:key record))))))))))

(defn- find-records-by-kind [db kind filters sorts limit offset]
  (let [keys (redis/with-connection db (redis/keys (str kind ":*")))
        results (map #(find-record-by-key db %) keys)]
    (->> results
      (filter (memory/build-filter filters))
      (sort/sort-results sorts)
      (filter/offset-results offset)
      (filter/limit-results limit))))

(defn- count-records-by-kind [db kind filters]
  (count (find-records-by-kind db kind filters nil nil nil)))

(defn- delete-records-by-kind [db kind filters]
  (let [records (find-records-by-kind db kind filters nil nil nil)]
    (map #(delete-record-by-key (:key %)) records)))

(defn- all-record-kinds [db]
  (redis/with-connection db
    (redis/smembers all-kinds-key)))

(deftype RedisDatastore [db]
  Datastore
  (ds-save [this records] (save-records db records))
  (ds-delete-by-kind [this kind filters] (delete-records-by-kind db kind filters))
  (ds-delete-by-key [this key] (delete-record-by-key db key) nil)
  (ds-count-by-kind [this kind filters] (count-records-by-kind db kind filters))
  (ds-find-by-key [this key] (find-record-by-key key))
  (ds-find-by-kind [this kind filters sorts limit offset] (find-records-by-kind db kind filters sorts limit offset))
  (ds-all-kinds [this] (all-record-kinds db))
  (ds-pack-key [this value] value)
  (ds-unpack-key [this kind value] value))

(defn new-redis-datastore [& args]
  (let [options (->options args)
        db (db-specification options)]
    (RedisDatastore. db)))
