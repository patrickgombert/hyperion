(ns hyperion.redis
  (:require [hyperion.abstr :refer [Datastore]]
            [hyperion.key :refer (generate-id)]
            [hyperion.sorting :as sort]
            [hyperion.filtering :as filter]
            [hyperion.memory :as memory]
            [accession.core :as redis]
            [chee.util :refer [->options]]))

(defmacro with-redis [db & body]
     `(redis/with-connection ~db ~@body))

(defn- open-db [options]
  (redis/connection-map options))

(def kinds-key "hyperion-kinds")

(defn- build-key [kind]
  (str kind (generate-id)))

(defn- kind-from-key [key]
  (first (clojure.string/split "some:thing" #":")))
;
;(defn- record-key [record]
;  (let [key  (:key  record)]
;    (hyperion-key key)))
;
;(defn- keys-of-kind [db kind]
;  (carmine db
;    (r/smembers (kind-key kind))))
;
;;; SAVING
;
;(defn- add-keys-to-kind [db keys kind]
;  (carmine db
;    (doseq [key keys] (r/sadd (kind-key kind) key))
;    (r/sadd (str hyperion-namespace "kinds") kind)))
;
(defn- persist-record [db record]
  (let [record-key (:key record)
        keyless-record (dissoc record :key)
        stringified-record (zipmap (map name (keys keyless-record)) (vals record))]
    (with-redis db
      (redis/hmset record-key "key" record-key (flatten (seq stringified-record))))
  record))

(defn- create-record [db record]
  (let [kind (:kind record)]
    (with-redis db
      (redis/sadd kinds-key kind))
    (persist-record db
      (assoc record :key (build-key kind)))))

(defn- save-records [db records]
  (let [new-records (filter #(nil? (:key %)) records)
        existing-records (filter #(not (nil? (:key %))) records)]
    (into
      (map #(create-record db %) new-records)
      (map #(persist-record db %) existing-records))))

;  (let [inserts (filter #(nil? (:key %)) records)
;        updates (filter :key records)
;        insert-groups (group-by :kind inserts)]
;    (doall
;      (concat
;        (map (partial save-record db) updates)
;        (mapcat
;          (fn [[kind values]] (insert-records-of-kind db kind values))
;          insert-groups)))))
;
;;; FINDING
;
(defn- symbolize-keys [str-map]
  (zipmap (map keyword (keys str-map)) (vals str-map)))

(defn- find-by-key [db key]
  (with-redis db
    (symbolize-keys (apply hash-map (redis/hgetall key)))))

;(defn- find-by-kind [db kind filters sorts limit offset]
;  (let [keys (keys-of-kind db kind)
;        results (map (partial find-by-key db) keys)]
;    (->> results
;      (filter (memory/build-filter filters))
;      (sort/sort-results sorts)
;      (filter/offset-results offset)
;      (filter/limit-results limit))))
;
(defn- delete-by-key [db key]
  (let [kind (kind-from-key key)]
    (with-redis db
      (redis/del key))))
;
;(defn- delete-by-kind [db kind filters]
;  (let [keys (map :key (find-by-kind db kind filters nil nil nil))]
;    (doseq [key keys] (delete-by-key db key))))
;
;;; COUNTING
;
;(defn- count-by-kind [db kind filters]
;  (count (find-by-kind db kind filters nil nil nil)))
;
;;; LISTING
;
(defn- list-all-kinds [db]
  (with-redis db
    (redis/smembers kinds-key)))

;; DATASTORE

(deftype RedisDatastore [db]
  Datastore
  (ds-save [this records] (save-records db records))
  (ds-delete-by-kind [this kind filters] true);(delete-by-kind db kind filters))
  (ds-delete-by-key [this key] (delete-by-key db key))
  (ds-count-by-kind [this kind filters] 0);(count-by-kind db kind filters))
  (ds-find-by-key [this key] (find-by-key db key))
  (ds-find-by-kind [this kind filters sorts limit offset] []);(find-by-kind db kind filters sorts limit offset))
  (ds-all-kinds [this] (vec (list-all-kinds db)))
  (ds-pack-key [this value] value)
  (ds-unpack-key [this kind value] value))

(defn new-redis-datastore [& args]
  (let [options (->options args)
        db (open-db options)]
    (RedisDatastore. db)))
