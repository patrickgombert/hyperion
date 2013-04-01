(ns hyperion.redis.meta-record
  (:require [clojure.data.json :as json]))

(defmulti db-type type)
(defmethod db-type java.lang.String [_] "String")
(defmethod db-type java.lang.Long [_] "Integer")
(defmethod db-type java.lang.Integer [_] "Integer")
(defmethod db-type java.lang.Number [_] "Integer")
(defmethod db-type java.lang.Double [_] "Number")
(defmethod db-type java.lang.Float [_] "Number")
(defmethod db-type java.lang.Boolean [_] "Boolean")
(defmethod db-type nil [_] "Null")
(defmethod db-type clojure.lang.PersistentVector [_] "Array")
(defmethod db-type clojure.lang.PersistentList$EmptyList [_] "Array")
(defmethod db-type clojure.lang.PersistentList [_] "Array")
(defmethod db-type clojure.lang.PersistentArrayMap [_] "Object")
(defmethod db-type :default [_] "Any")

(defmulti from-db-type (fn [_ value-type] value-type))
(defmethod from-db-type "String" [value _] value)
(defmethod from-db-type "Integer" [value _] (Integer/parseInt value))
(defmethod from-db-type "Number" [value _] (Double/parseDouble value))
(defmethod from-db-type "Boolean" [value _] (Boolean/parseBoolean value))
(defmethod from-db-type "Null" [value _] nil)
(defmethod from-db-type "Array" [value _] (json/read-str value))
(defmethod from-db-type "Object" [value _] (json/read-str value))
(defmethod from-db-type "Any" [value _] value)

(defn meta-key [key]
  (str "__metadata__" key))

(defn meta-record [record]
  (zipmap (keys record) (map #(db-type %) (vals record))))

(defn cast-record [raw-record meta-record]
  (reduce
    (fn [acc raw-pair]
      (let [pair-key (first raw-pair)
            pair-value (second raw-pair)
            value-type (get meta-record pair-key)]
        (assoc acc pair-key (from-db-type pair-value value-type)))) {} raw-record))
