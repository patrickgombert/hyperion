(ns hyperion.api
  (:require [clojure.string :as str]
            [hyperion.abstr :refer :all ]
            [hyperion.filtering :as filter]
            [hyperion.log :as log]
            [hyperion.sorting :as sort]
            [chee.datetime :refer [now]]
            [chee.util :refer [->options]])
  (:import  [java.lang IllegalArgumentException]))

(declare ^{:dynamic true
           :tag hyperion.abstr.Datastore
           :doc "Stores the active datastore."} *ds*)

(defn set-ds!
  "Uses alter-var-root to set *ds*. A violent, but effective way to install a datastore."
  [^hyperion.abstr.Datastore ds]
  (log/debug "forcefully setting datastore:" ds)
  (alter-var-root (var *ds*) (fn [_] ds)))

(defn ds
  "Returns the currently bound datastore instance"
  []
  (if (and (bound? #'*ds*) *ds*)
    *ds*
    (throw (NullPointerException. "No Datastore bound (hyperion/*ds*). Use clojure.core/binding to bind a value or hyperion.api/set-ds! to globally set it."))))

(defn new?
  "Returns true if the record is new (not saved/doesn't have a :key), false otherwise."
  [record]
  (nil? (:key record)))

(defn- if-assoc [map key value]
  (if value
    (assoc map key value)
    map))

; ----- Hooks ---------------------------------------------

(defmulti after-create
  "Hook to alter an entity immediately after being created"
  #(keyword (:kind %)))
(defmethod after-create :default [record] record)

(defmulti before-save
  "Hook to alter values immediately before being saved
  " #(keyword (:kind %)))
(defmethod before-save :default [record] record)

(defmulti after-load
  "Hook to alter values immediately after being loaded"
  #(keyword (:kind %)))
(defmethod after-load :default [record] record)

; ----- Entity Implementation -----------------------------

(defmulti pack
  "Packers may be any object and are added to defentity specs.
When an entity is saved, values are 'packed' before getting shipped
off to the persistence implementation.
You may add your own packer by declaring a defmethod for your type."
  (fn [type value] type))
(defmethod pack :default [type value] value)

(defmulti unpack
  "Unpackers may be any object and are added to defentity specs.
When an entity is loaded, values are 'unpacked' from the data in the
persistence implementation.
You may add your own packer by declaring a defmethod for your type."
  (fn [type value] type))
(defmethod unpack :default [type value] value)

(defn- apply-type-packers [options]
  (if-let [t (:type options)]
    (-> (dissoc options :type )
      (assoc :packer t)
      (assoc :unpacker t))
    options))

(defn- normalize-db-name [field options]
  (let [db-name (if-let [name (:db-name options)] (->field name) field)]
    (assoc options :db-name db-name)))

(defn- map-fields [fields]
  (reduce
    (fn [spec [key & args]]
      (let [field (->field key)
            options (->options args)
            options (apply-type-packers options)
            options (normalize-db-name field options)]
        (assoc spec field options)))
    {}
    fields))

(defn- apply-default [entity field-name field-spec]
  (if (contains? entity field-name)
    (get entity field-name)
    (:default field-spec)))

(defn construct-entity
  "PRIVATE: Used by the defentity macro to create entities."
  [entity]
  (after-create
    (if-let [spec (spec-for (->kind entity))]
      (reduce
        (fn [created [field-name field-spec]]
          (assoc created field-name (apply-default entity field-name field-spec)))
        entity
        spec)
      (throw (Exception. (format "Missing entity spec in constructor for %s." (->kind entity)))))))

(defn- packer-fn [packer]
  (if (fn? packer)
    packer
    #(pack packer %)))

(defn- unpacker-fn [packer]
  (if (fn? packer)
    packer
    #(unpack packer %)))

(defn- do-packing [packer value]
  (if (or (sequential? value) (isa? (class value) java.util.List))
    (reduce
      (fn [acc item]
        (try
          (conj acc (packer item))
          (catch IllegalArgumentException e
            (log/warn (format "error packing %s: %s" (pr-str item) (.getMessage e)))
            acc)))
      []
      value)
    (packer value)))

(defn- pack-field-value [spec value]
  (do-packing (packer-fn (:packer spec)) value))

(defn- packed-field-name [spec field]
  (or (:db-name spec) field))

(defn- base-entity [entity]
  (if-let [key (:key entity)]
    {:key key}
    {}))

(defn packed-kind [entity]
  (if-let [kind (->kind entity)]
    (name kind)
    nil))

(defn- pack-fields [entity]
  (if-let [spec (spec-for entity)]
    (reduce
      (fn [packed [field-name field-spec]] (assoc packed (packed-field-name field-spec field-name) (pack-field-value field-spec (get entity field-name))))
      (assoc (base-entity entity) :kind (packed-kind entity))
      spec)
    (assoc entity :kind (packed-kind entity))))

(defn- unpack-field-value [spec value]
  (do-packing (unpacker-fn (:unpacker spec)) value))

(defn- unpack-fields [entity]
  (if-let [spec (spec-for entity)]
    (reduce
      (fn [unpacked [field-name field-spec]] (assoc unpacked field-name (unpack-field-value field-spec (get entity (packed-field-name field-spec field-name)))))
      (assoc (base-entity entity) :kind (->kind entity))
      spec)
    (assoc entity :kind (->kind entity))))

(defn- normalize-field-names [record]
  (reduce
    (fn [record [field value]] (assoc record (->field field) value))
    {}
    record))

(defn- with-created-at [record spec]
  (if (and (or (contains? spec :created-at ) (contains? record :created-at )) (= nil (:created-at record)))
    (assoc record :created-at (now))
    record))

(defn- with-updated-at [record spec]
  (if (or (contains? spec :updated-at ) (contains? record :updated-at ))
    (assoc record :updated-at (now))
    record))

(defn- with-updated-timestamps [record]
  (let [spec (spec-for record)]
    (-> record
      (with-created-at spec)
      (with-updated-at spec))))

(defn- unpack-entity [entity]
  (when entity
    (-> entity
      normalize-field-names
      unpack-fields
      after-load)))

(defn- ensure-entity-has-kind [entity]
  (if-not (contains? entity :kind )
    (throw (Exception. (str "Cannot save without specifying a kind: " entity)))
    entity))

(defn- pack-entity [entity]
  (when entity
    (-> entity
      ensure-entity-has-kind
      with-updated-timestamps
      before-save
      pack-fields)))

(defmacro defentity
  "Used to define entities. An entity is simply an encapulation of data that
  is persisted.
  The advantage of using entities are:
   - they limit the fields persisted to only what is specified in their
     definition.
   - default values can be assigned to fields
   - types, packers, and unpackers can be assigned to fields.  Packers
     allow you to manipulate a field (perhaps serialize it) before it
     is persisted.  Unpacker conversly manipulate fields when loaded.
     Packers and unpackers maybe a fn (which will be excuted) or an
     object used to pivot the pack and unpack multimethods.
     A type (object) is simply a combined packer and unpacker.
   - constructors are provided
   - they are represented by records (defrecord) instead of plain maps.
     This allows you to use extend-type on them if you choose.

   Example:

      (defentity Citizen
        [name]
        [age :packer ->int] ; ->int is a function defined in your code.
        [gender :unpacker ->string] ; ->string is a customer function too.
        [occupation :type my.ns.Occupation] ; and then we define pack/unpack for my.ns.Occupation
        [spouse-key :type (foreign-key :citizen)] ; this is a special type that packs string keys into implementation-specific keys
        [country :default \"USA\"] ; newly created records will use the default if no value is provided
        [created-at] ; populated automaticaly
        [updated-at] ; also populated automatically
        )

        (save (citizen :name \"John\" :age \"21\" :gender :male :occupation coder :spouse-key \"abc123\"))

        ;=> #<Citizen {:kind \"citizen\" :key \"some generated key\" :country \"USA\" :created-at #<java.util.Date just-now> :updated-at #<java.util.Date just-now> ...)
  "
  [class-sym & fields]
  (let [field-map (map-fields fields)
        kind (->kind class-sym)
        kind-key (keyword kind)
        kind-fn (symbol kind)]
    `(do
       (dosync (alter *entity-specs* assoc ~(keyword kind) ~field-map))
       (defn ~kind-fn [& args#] (construct-entity (assoc (->options args#) :kind ~kind))))))

; ----- API -----------------------------------------------

(defn save
  "Saves a record. Any additional parameters will get merged onto the record
  before it is saved.

    (save {:kind :foo})
    ;=> {:kind \"foo\" :key \"generated key\"}
    (save {:kind :foo} {:value :bar})
    ;=> {:kind \"foo\" :value :bar :key \"generated key\"}
    (save {:kind :foo} :value :bar)
    ;=> {:kind \"foo\" :value :bar :key \"generated key\"}
    (save {:kind :foo} {:value :bar} :another :fizz)
    ;=> {:kind \"foo\" :value :bar :another :fizz :key \"generated key\"}
    (save (citizen) :name \"Joe\" :age 21 :country \"France\")
    ;=> #<Citizen {:kind \"citizen\" :name \"Joe\" :age 21 :country \"France\" ...}>
  "
  [record & args]
  (let [attrs (->options args)
        record (merge record attrs)
        entity (pack-entity record)
        saved (first (ds-save (ds) [entity]))]
    (unpack-entity saved)))

(defn save*
  "Saves multiple records at once."
  [& records]
  (doall (map unpack-entity (ds-save (ds) (map pack-entity records)))))

(defn- ->filter-operator [operator]
  (case (name operator)
    ("=" "eq") := ("<" "lt") :< ("<=" "lte") :<= (">" "gt") :> (">=" "gte") :>= ("!=" "not") :!= ("contains?" "contains" "in?" "in") :contains? (throw (Exception. (str "Unknown filter operator: " operator)))))

(defn- ->sort-direction [dir]
  (case (name dir)
    ("asc" "ascending") :asc ("desc" "descending") :desc (throw (Exception. (str "Unknown sort direction: " dir)))))

; Protocol?
(defn- ->seq [items]
  (cond
    (nil? items) []
    (coll? (first items)) items
    :else [items]))

(defn- parse-filters [kind filters]
  (let [spec (spec-for kind)
        filters (->seq filters)]
    (doall
      (map
        (fn [[operator field value]]
          (let [field (->field field)
                spec (field spec)]
            (filter/make-filter
              (->filter-operator operator)
              (packed-field-name spec field)
              (pack-field-value spec value))))
        filters))))

(defn- parse-sorts
  ([kind sorts]
    (let [spec (spec-for kind)
          sorts (->seq sorts)]
      (doall
        (map
          (fn [[field direction]]
            (let [field (->field field)
                  spec (field spec)]
              (sort/make-sort
                (packed-field-name spec field)
                (->sort-direction direction))))
          sorts))))
  ([sorts]
    (let [sorts (->seq sorts)]
      (doall
        (map
          (fn [[field direction]]
              (sort/make-sort
                (->field field)
                (->sort-direction direction)))
          sorts)))))

(defn- find-records-by-kind [kind filters sorts limit offset]
  (map unpack-entity (ds-find-by-kind (ds) kind (parse-filters kind filters) (parse-sorts kind sorts) limit offset)))

(defn- key-present? [key]
  (not (or (nil? key) (str/blank? (str key)))))

(defn find-by-key
  "Retrieves the value associated with the given key from the datastore.
nil if it doesn't exist."
  [key]
  (try
    (unpack-entity
      (when (key-present? key)
        (ds-find-by-key (ds) key)))
    (catch IllegalArgumentException e
      (log/warn (format "find-by-key error: %s" (.getMessage e)))
      nil)))

(defn reload
  "Returns a freshly loaded record based on the key of the given record."
  [entity]
  (find-by-key (:key entity)))

(defn find-by-kind
  "Returns all records of the specified kind that match the filters provided.

    (find-by-kind :dog) ; returns all records with :kind of \"dog\"
    (find-by-kind :dog :filters [:= :name \"Fido\"]) ; returns all dogs whos name is Fido
    (find-by-kind :dog :filters [[:> :age 2][:< :age 5]]) ; returns all dogs between the age of 2 and 5 (exclusive)
    (find-by-kind :dog :sorts [:name :asc]) ; returns all dogs in alphebetical order of their name
    (find-by-kind :dog :sorts [[:age :desc][:name :asc]]) ; returns all dogs ordered from oldest to youngest, and gos of the same age ordered by name
    (find-by-kind :dog :limit 10) ; returns upto 10 dogs in undefined order
    (find-by-kind :dog :sorts [:name :asc] :limit 10) ; returns upto the first 10 dogs in alphebetical order of their name
    (find-by-kind :dog :sorts [:name :asc] :limit 10 :offset 10) ; returns the second set of 10 dogs in alphebetical order of their name

  Filter operations and acceptable syntax:
    := \"=\" \"eq\"
    :< \"<\" \"lt\"
    :<= \"<=\" \"lte\"
    :> \">\" \"gt\"
    :>= \">=\" \"gte\"
    :!= \"!=\" \"not\"
    :contains? \"contains?\" :contains \"contains\" :in? \"in?\" :in \"in\"

  Sort orders and acceptable syntax:
    :asc \"asc\" :ascending \"ascending\"
    :desc \"desc\" :descending \"descending\"
  "
  [kind & args]
  (try
    (let [options (->options args)
          kind (name kind)]
      (find-records-by-kind kind
        (:filters options)
        (:sorts options)
        (:limit options)
        (:offset options)))
    (catch IllegalArgumentException e
      (log/warn (format "find-by-kind error: %s" (.getMessage e)))
      [])))

(defn find-all-kinds
  "Same as find-by-kind except that it'll returns results of any kind
WARNING: This method is almost certainly horribly inefficient.  Use with caution."
  [& args]
  (let [options (->options args)
        kinds (ds-all-kinds (ds))
        sorts (parse-sorts (:sorts options))
        filters (:filters options)
        results (flatten (map #(find-records-by-kind % filters nil nil nil) kinds))]
    (->> results
      (filter #(not (nil? %)))
      (sort/sort-results (parse-sorts (:sorts options)))
      (filter/offset-results (:offset options))
      (filter/limit-results (:limit options)))))

(defn- count-records-by-kind [kind filters]
  (ds-count-by-kind (ds) kind (parse-filters kind filters)))

(defn count-by-kind
  "Counts records of the specified kind that match the filters provided."
  [kind & args]
  (try
    (let [options (->options args)
          kind (name kind)]
      (count-records-by-kind kind (:filters options)))
    (catch IllegalArgumentException e
      (log/warn (format "count-by-kind error: %s" (.getMessage e)))
      0)))

(defn- count-records-by-all-kinds [filters]
  (let [kinds (ds-all-kinds (ds))
        results (flatten (map #(count-records-by-kind % filters) kinds))]
    (apply + results)))

(defn count-all-kinds
  "Counts records of any kind that match the filters provided."
  [& args]
  (let [options (->options args)]
    (count-records-by-all-kinds (:filters options))))

(defn delete-by-key
  "Removes the record stored with the given key.
Returns nil no matter what."
  [key]
  (try
    (when (key-present? key)
      (ds-delete-by-key (ds) key)
      nil)
    (catch IllegalArgumentException e
      (log/warn (format "find-by-key error: %s" (.getMessage e)))
      nil)))

(defn delete-by-kind
  "Deletes all records of the specified kind that match the filters provided."
  [kind & args]
  (try
    (let [options (->options args)
          kind (->kind kind)]
      (ds-delete-by-kind (ds) kind (parse-filters kind (:filters options)))
      nil)
    (catch IllegalArgumentException e
      (log/warn (format "delete-by-kind error: %s" (.getMessage e)))
      nil)))

; ----- Factory -------------------------------------------

(defn new-datastore
  "Factory methods to create datastore instances.  Just provide the
  :implementation you want (along with configuration) and we'll load
  the namespace and construct the instance for you.

    (new-datastore :implementation :memory) ; create a new in-memory datastore
    (new-datastore :implementation :sqlite :connection-url \"jdbc:sqlite:\") ; creates a new sqlite datastore
  "
  [& args]
  (let [options (->options args)]
    (if-let [implementation (:implementation options)]
      (try
        (let [ns-sym (symbol (str "hyperion." (name implementation)))]
          (require ns-sym)
          (let [constructor-sym (symbol (format "new-%s-datastore" (name implementation)))
                constructor (ns-resolve (the-ns ns-sym) constructor-sym)
                datastore (constructor options)]
            (log/debug "new-datastore.  config:" options "datastore:" datastore)
            datastore))
        (catch java.io.FileNotFoundException e
          (throw (Exception. (str "Can't find datastore implementation: " implementation) e))))
      (throw (Exception. "new-datastore requires an :implementation entry (:memory, :mysql, :mongo, ...)"))
      )))
