(ns biiwide.kvdb
  (:refer-clojure :exclude [key remove replace])
  (:require [biiwide.kvdb.protocols :as proto]
            [clojure.core :as core]
            [clojure.spec.alpha :as s])
  (:import (biiwide.kvdb.protocols ReadableKVDB
                                   MutableKVDB
                                   PageableKVDB)))


(defmacro ^:private maybe
  [spec]
  `(s/or :missing nil?
         :value   ~spec))


(s/fdef entry?
  :args (s/cat :any any?)
  :ret  boolean?)

(defn entry?
  "Test if a value is a KVDB Entry."
  [x]
  (proto/entry? x))


(s/def ::key string?)
(s/def ::value map?)
(s/def ::revision nat-int?)
(s/def ::entry entry?)

(s/def ::entry-accessor-args
  (s/cat :value (s/or :entry entry?
                      :any any?)))


(s/fdef key
  :args ::entry-accessor-args
  :ret  (maybe ::key))

(defn key
  "Return the key of a KVDB Entry."
  [entry]
  (when (entry? entry)
    (proto/key entry)))


(s/fdef value
  :args ::entry-accessor-args
  :ret  (maybe ::value))

(defn value
  "Return the value of a KVDB Entry."
  [entry]
  (when (entry? entry)
    (proto/value entry)))


(s/fdef revision
  :args ::entry-accessor-args
  :ret  (maybe ::revision))

(defn revision
  "Return the revision of a KVDB Entry."
  [entry]
  (when (entry? entry)
    (proto/revision entry)))


(s/def ::entry-with-requested-key
  #(= (:key (:args %))
      (key (:ret %))))


(s/fdef entry
  :args (s/cat :key ::key :value ::value :revision ::revision)
  :ret  ::entry
  :fn   (s/and #(= (:key (:args %))
                   (key (:ret %)))
               #(= (:value (:args %))
                   (value (:ret %)))
               #(= (:revision (:args %))
                   (revision (:ret %)))))

(defn entry
  "Construct a KVDB Entry."
  [key value revision]
  (vary-meta value assoc
             :type ::entry
             ::key key
             ::revision revision))


(s/fdef fmap-entry
  :args (s/cat :fn ifn? :entry ::entry)
  :ret  ::entry
  :fn   (s/and #(= (key (:entry (:args %)))
                   (key (:ret %)))
               #(= (revision (:entry (:args %)))
                   (revision (:ret %)))))

(defn fmap-entry
  "Apply a function the value of an entry."
  [f ent]
  (entry (key ent)
         (f (value ent))
         (revision ent)))


(s/fdef kvdb?
  :args (s/cat :any any?)
  :ret  boolean?)

(defn kvdb?
  "Checks if a value implements the ReadableKVDB protocol."
  [x]
  (proto/readable-kvdb? x))

(s/fdef kvdbable?
  :args (s/cat :any any?)
  :ret  boolean?)

(defn kvdbable?
  [x]
  (satisfies? proto/KVDBable x))


(defn to-kvdb
  "Coerce a potential KVDB value into a full KVDB."
  [x]
  (cond (kvdb? x)     x
        (kvdbable? x) (proto/to-kvdb x)
        :else
        (throw (IllegalArgumentException.
                  (format "Cannot create a KVDB from %s" (class x))))))


(s/fdef readable-kvdb?
  :args (s/cat :any any?)
  :ret  boolean?)

(defn readable-kvdb?
  "Checks if a value implements the ReadableKVDB protocol."
  [x]
  (proto/readable-kvdb? x))


(s/def ::kvdb readable-kvdb?)
(s/def ::readable-kvdb readable-kvdb?)


(s/fdef fetch
  :args (s/cat :kvdb          ::readable-kvdb
               :key           ::key
               :missing-value (s/? any?))
  :ret  (s/or :entry         ::entry
              :missing-value any?)
  :fn   #(let [[retk retv] (:ret %)]
           (case retk
             :entry         true #_(= (:key (:args %)) (key retv))
             :missing-value (identical? (:missing-value (:args %))
                                        retv))))

(defn fetch
  "Fetch an entry from a ReadableKVDB for the given key.
Either nil or a provided missing-value will be returned
when the entry is not found."
  ([readable-kvdb key]
    (fetch readable-kvdb key nil))
  ([readable-kvdb key missing-value]
    (proto/fetch readable-kvdb key missing-value)))


(s/fdef entries
  :args (s/cat :kvdb ::readable-kvdb)
  :ret  (s/nilable (s/every ::entry)))

(defn entries
  "Return a sequence of all entries in a ReadableKVDB."
  [readable-kvdb]
  (proto/entries readable-kvdb))


(s/fdef mutable-kvdb?
  :args (s/cat :any any?)
  :ret  boolean?)

(defn mutable-kvdb?
  "Checks if a value satifsfies both the ReadableKVDB and
 MutableKVDB protocols."
  [x]
  (and (readable-kvdb? x)
       (proto/mutable-kvdb? x)))


(s/def ::mutable-kvdb mutable-kvdb?)


(s/fdef create!
  :args (s/cat :kvdb   ::mutable-kvdb
               :key    ::key
               :value  ::value)
  :ret  ::entry
  :fn   ::entry-with-requested-key)

(defn create!
  "Create an entry in a MutableKVDB instance
if and only if it an entry for the key does not
already exist."
  [mutable-kvdb key value]
  (proto/create! mutable-kvdb key value))


(s/fdef remove!
  :args (s/cat :kvdb     ::mutable-kvdb
               :key      ::key
               :revision ::revision)
  :ret  ::entry
  :fn   ::entry-with-requested-key)

(defn remove!
  "Remove an entry from a MutableKVDB instance
if an only if the entry exists and it's revision number
matches the revision number provided."
  [mutable-kvdb key revision]
  (proto/remove! mutable-kvdb key revision))


(s/fdef replace!
  :args (s/cat :kvdb      ::mutable-kvdb
               :key       ::key
               :revision  ::revision
               :new-value ::value)
  :ret  ::entry
  :fn   ::entry-with-requested-key)

(defn replace!
  "Replace an entry in a MutableKVDB instance with a
new value if an only if the entry exists and it's revision
number matches the revision number provided."
  [mutable-kvdb key revision new-value]
  (proto/replace! mutable-kvdb key revision new-value))


(s/def ::action
  #{::create! ::replace! ::remove!})


(s/fdef precondition-exception?
  :args (s/cat :any any?)
  :ret  boolean?)

(defn precondition-exception?
  [ex]
  (isa? (:type (ex-data ex))
        ::precondition-failure))


(derive ::key-collision ::precondition-failure)


(s/fdef key-collision-exception?
  :args (s/cat :any any?)
  :ret  boolean?)

(defn key-collision-exception?
  "Test for a key-collision exception."
  [ex]
  (= ::key-collision
     (:type (ex-data ex))))


(s/fdef key-collision
  :args (s/cat :kvdb ::kvdb :action ::action :key ::key)
  :ret  key-collision-exception?)

(defn key-collision
  "Construct a key collision exception.
This exception is thrown when attempting to create! a new entry."
  [kvdb action k]
  (ex-info "Key already exists and cannot be created."
           {:type   ::key-collision
            :kvdb   kvdb
            :action action
            :key    k}))


(derive ::revision-mismatch ::precondition-failure)


(s/fdef revision-mismatch-exception?
  :args (s/cat :any any?)
  :ret  boolean?)

(defn revision-mismatch-exception?
  "Test for a revision-mismatch exception."
  [ex]
  (= ::revision-mismatch
     (:type (ex-data ex))))


(s/fdef revision-mismatch
  :args (s/cat :kvdb              ::kvdb
               :action            ::action
               :key               ::key
               :actual-revision   ::revision
               :expected-revision ::revision)
  :ret  revision-mismatch-exception?)

(defn revision-mismatch
  "Construct a revision mismatch exception.
This exception is thrown when attempting to replace! or
remove! an entry where the current revision does not match
the supplied revision.
This typically happens when a concurrent modification to
the entry has occured."
  [kvdb action k actual-rev expected-rev]
  (ex-info "Actual revision did not match expected revision."
           {:type              ::revision-mismatch
            :kvdb              kvdb
            :action            action
            :key               k
            :actual-revision   actual-rev
            :expected-revision expected-rev}))


(derive ::key-not-found ::precondition-failure)


(s/fdef key-not-found-exception?
  :args (s/cat :any any?)
  :ret  boolean?)

(defn key-not-found-exception?
  "Test for a key-not-found exception."
  [ex]
  (= ::key-not-found
     (:type (ex-data ex))))


(s/fdef key-not-found
  :args (s/cat :kvdb              ::kvdb
               :action            ::action
               :key               ::key)
  :ret  key-not-found-exception?)

(defn key-not-found
  "Construct a key not found exception.
This exception is throw when attempting to replace! or
remove! an entry that does not exist."
  [kvdb action k]
  (ex-info "Key not found in database."
           {:type   ::key-not-found
            :kvdb   kvdb
            :action action
            :key    k}))


(definline ^:private exception?
  [x]
  `(instance? Exception ~x))


(s/fdef exceeded-transact-attempts-exception?
  :args (s/cat :any any?)
  :ret  boolean?)

(defn exceeded-transact-attempts-exception?
  "Test for a exceeded-transact-attempts exception."
  [ex]
  (= ::exceeded-transact-attempts
     (:type (ex-data ex))))


(s/fdef exceeded-transact-attempts
  :args (s/cat :kvdb     ::kvdb
               :key      ::key
               :tx       ifn?
               :attempts nat-int?
               :cause    (s/? exception?))
  :ret  exceeded-transact-attempts-exception?)

(defn exceeded-transact-attempts
  "Construct an exceeded transact attempts exception.
This exception is thrown when an attempt to transact! an
entry exceeds the maximum number of attempts due to
concurrent modifications."
  [kvdb k f attempts & [cause]]
  (ex-info "Exceeded maximum transact attempts."
           {:type     ::exceeded-transact-attempts
            :kvdb     kvdb
            :action   ::transact-values!
            :key      k
            :tx       f
            :attempts attempts}
            cause))


(s/fdef removal?
  :args (s/cat :any any?)
  :ret  boolean?)

(defn removal?
  "Test for a removal token."
  [x]
  (identical? ::removal x))


(s/fdef removal
  :args (s/cat :any any?)
  :ret  removal?)

(defn removal
  "Returns a token indicating a transacted value should be removed.
Examples:
 (kvdb/transact! db \"abc\" kvdb/removal)
 (kvdb/transact! db \"def\"
   (fn [v] (if (even? (:xyz v))
             (kvdb/removal v)
             v)))"
  [_]
  ::removal)


(defn ^:private apply-tx
  [kvdb k f]
  (let [missing-v (Object.)
        ent (proto/fetch kvdb k missing-v)
        missing? (identical? missing-v ent)
        old-val (if missing? nil (value ent))
        new-val (f old-val)]
    (cond missing?
          (if (removal? new-val)
            [nil nil]
            (try [nil (create! kvdb k new-val)]
              (catch Exception ex
                (cond (precondition-exception? ex) ex
                      :else (throw ex)))))
          (removal? new-val)
          (try [(remove! kvdb k (revision ent)) nil]
            (catch Exception ex
              (cond (precondition-exception? ex) ex
                    :else (throw ex))))
          (= old-val new-val)
          [old-val old-val]
          :else
          (try [ent (replace! kvdb k (revision ent) new-val)]
            (catch Exception ex
              (cond (precondition-exception? ex) ex
                    :else (throw ex)))))))


(def ^:dynamic *max-transact-attempts* 50)


(s/fdef transact-values!
  :args (s/cat :kvdb ::mutable-kvdb
               :key  ::key
               :tx   ifn?
               :args (s/* any?))
  :ret  (s/cat :old-entry (maybe ::entry)
               :new-entry (maybe ::entry))
  :fn   (s/and #(let [[ret-tag ret-val] (:old-entry (:ret %))]
                  (case ret-tag
                    :value   (= (:key (:args %)) (key ret-val))
                    :missing (nil? ret-val)))
               #(let [[ret-tag ret-val] (:new-entry (:ret %))]
                  (case ret-tag
                    :value   (= (:key (:args %)) (key ret-val))
                    :missing (nil? ret-val)))))

(defn transact-values!
  "Atomically updates the value of an entry in a KVDB instance.
  The new value will be (apply f current-value-of-entry args). The
  trasacting function f may be called multiple times and should be
  free of side effects.
  When an entry is not present, the current-value-of-entry will be nil.
  To remove an entry f can return (removal).
  Returns [old-entry new-entry], the state of the entry before and
  after the update.
  Analogous to clojure.core/swap-vals!."
  ([kvdb k f a]
   (transact-values! kvdb k #(f % a)))
  ([kvdb k f a b]
   (transact-values! kvdb k #(f % a b)))
  ([kvdb k f a b c]
   (transact-values! kvdb k #(f % a b c)))
  ([kvdb k f a b c & more-args]
   (transact-values! kvdb k #(apply f % a b c more-args)))
  ([kvdb k f]
   (let [max-attempts *max-transact-attempts*]
     (loop [remaining-attempts max-attempts
            last-exception     nil]
       (if (pos? remaining-attempts)
         (let [result (apply-tx kvdb k f)]
           (if (exception? result)
             (do (Thread/sleep 0 (rand-int 999))
                 (recur (dec remaining-attempts) result))
             result))
         (throw
           (exceeded-transact-attempts
             kvdb k f max-attempts last-exception)))))))


(s/fdef transact!
  :args (s/cat :kvdb ::mutable-kvdb
               :key  ::key
               :tx   ifn?
               :args (s/* any?))
  :ret  (maybe ::entry)
  :fn   #(let [[ret-tag ret-val] (:ret %)]
          (case ret-tag
            :value   (= (:key (:args %)) (key ret-val))
            :missing (nil? ret-val))))


(def transact!
  "Atomically updates the value of an entry in a KVDB instance.
  The new value will be (apply f current-value-of-entry args). The
  trasacting function f may be called multiple times and should be
  free of side effects.
  Returns the new, updated state of the entry.
  Analogous to clojure.core/swap!."
  (comp second transact-values!))


(s/fdef set-values!
  :args (s/cat :kvdb ::mutable-kvdb
               :key  ::key
               :val  ::value)
  :ret  (s/cat :old-entry (maybe ::entry)
               :new-entry ::entry)
  :fn   (s/and #(let [[tag old-entry] (:old-entry (:ret %))]
                  (case tag
                    :value   (= (:key (:args %)) (key old-entry))
                    :missing (nil? old-entry)))
               #(let [new-entry (:new-entry (:ret %))]
                  (and (= (:key (:args %)) (key new-entry))
                       (= (:val (:args %)) (value new-entry))))))


(defn set-values!
  "Set the value for a key in the database without regard for the
  current state of the entry.
  Returns [old-entry new-entry], the state of the entry before and
  after it was set.
  Equivalent to (transact-values! kvdb k (constantly v)).
  Analogous to clojure.core/reset-vals!."
  [kvdb k v]
  (transact-values! kvdb k (constantly v)))


(s/fdef set!
  :args (s/cat :kvdb ::mutable-kvdb
               :key  ::key
               :val  ::value)
  :ret  ::entry
  :fn   #(let [ret-val (:ret %)]
           (and (= (:key (:args %)) (key ret-val))
                (= (:val (:args %)) (value ret-val)))))


(def set!
  "Set the value for a key in the database without regard for the
  current state of the entry.
  Returns [old-entry new-entry], the state of the entry before and
  after it was set.
  Equivalent to (transact! kvdb k (constantly v)).
  Analogous to clojure.core/reset!."
  (comp second set-values!))


(defmacro with-max-transact-attempts
  "Override the default maximum transaction attempts within a scope."
  [max-attempts & body]
  (let [check (cond (number? max-attempts)
                    (assert (pos? max-attempts)
                            "Attempts must be a positive integer")
                    (symbol? max-attempts)
                    `((assert (pos? ~max-attempts)
                              "Attempts must be a positive integer"))
                    :else (throw (IllegalArgumentException.
                                   (format "Invalid max-attempts %s"
                                           (pr-str max-attempts)))))]
  `(do ~@check
       (binding [*max-transact-attempts* ~max-attempts]
         ~@body))))


(s/fdef pageable-kvdb?
  :args (s/cat :any any?)
  :ret  boolean?)

(defn pageable-kvdb?
  "Checks if a value satifsfies both the ReadableKVDB and
 PageableKVDB protocols."
  [x]
  (and (readable-kvdb? x)
       (proto/pageable-kvdb? x)))


(s/def ::pageable-kvdb pageable-kvdb?)

(s/def ::page-limit nat-int?)

(s/fdef page
  :args (s/cat :kvdb ::pageable-kvdb
               :page (s/? (s/cat :exclusive-start-key (maybe ::key)
                                 :limit               (maybe ::page-limit))))
  :ret  (s/every ::entry)
  :fn   #(if-some [limit (second (:limit (:page (:args %))))]
          (<= (count (:ret %)) limit)
          true))

(defn page
  "Retrieve a \"page\" of entries from a PageableKVDB.
An exclusive-start-key of nil will start paging from the
first entry."
  ([pageable-kvdb]
    (page pageable-kvdb nil nil))
  ([pageable-kvdb exclusive-start-key page-limit]
    (proto/page pageable-kvdb exclusive-start-key page-limit)))


(s/fdef pageseq
  :args (s/cat :kvdb ::pageable-kvdb
               :page (s/? (s/cat :exclusive-start-key (maybe ::key)
                                 :page-limit          (maybe ::page-limit))))
  :ret  (s/every ::entry))

(defn pageseq
  "Returns a lazy sequence of entries using page."
  ([pageable-kvdb]
   (pageseq pageable-kvdb nil nil))
  ([pageable-kvdb exclusive-start-key page-limit]
   (lazy-seq
     (when-some [p (not-empty (page pageable-kvdb
                                    exclusive-start-key
                                    page-limit))]
        (let [last-key (key (last p))]
          (concat p (pageseq pageable-kvdb last-key page-limit)))))))


(s/fdef overridable-kvdb?
  :args (s/cat :any any?)
  :ret  boolean?)

(defn overridable-kvdb?
  "Check if a KVDB instance supports the OverridableKVDB protocol."
  [x]
  (and (readable-kvdb? x)
       (proto/overridable-kvdb? x)
       (not (instance? ReadableKVDB x))))

(s/def ::overridable-kvdb overridable-kvdb?)


(def ^:private readable-kvdb-methods
  #{`proto/fetch
    `proto/entries})

(s/def ::overridable-readable-kvdb-method
  readable-kvdb-methods)

(def ^:private mutable-kvdb-methods
  #{`proto/create!
    `proto/remove!
    `proto/replace!})

(s/def ::overridable-mutable-kvdb-method
  mutable-kvdb-methods)

(def ^:private pageable-kvdb-methods
  #{`proto/page})

(s/def ::overridable-pageable-kvdb-method
  pageable-kvdb-methods)

(def ^:private overridable-kvdb-methods
  (set (concat readable-kvdb-methods
               mutable-kvdb-methods
               pageable-kvdb-methods)))

(s/def ::overridable-kvdb-method
  overridable-kvdb-methods)


(s/fdef select-overridable-methods
  :args (s/cat :map map?)
  :ret  map?)

(defn select-overridable-methods
  [m]
  (select-keys m overridable-kvdb-methods))


(s/fdef without-overridable-methods
  :args (s/cat :map map?)
  :ret  map?)

(defn without-overridable-methods
  [m]
  (reduce dissoc m overridable-kvdb-methods))


(s/fdef override
  :args (s/cat :kvdb ::overridable-kvdb
               :impl (s/alt :single (s/cat :method ::overridable-kvdb-method
                                           :impl   ifn?)
                            :multiple (s/map-of ::overridable-kvdb-method ifn?)))
  :ret  ::overridable-kvdb)

(defn override
  ([kvdb protocol-method implementation]
   (override kvdb {protocol-method implementation}))
  ([kvdb implementations]
   (proto/push-overrides kvdb (select-overridable-methods implementations))))

(s/fdef overridden
  :args (s/cat :kvdb ::overridable-kvdb)
  :ret  (s/map-of ::overridable-kvdb-method fn?))

(defn overridden
  [kvdb]
  (when (overridable-kvdb? kvdb)
    (select-overridable-methods (proto/overridden kvdb))))


(s/fdef pop-overrides
  :args (s/cat :kvdb ::overridable-kvdb)
  :ret  ::overridable-kvdb)

(defn pop-overrides
  [kvdb]
  (proto/pop-overrides kvdb))


(defn ^:private enrich-map-entry
  [[k v]]
  (when-not (string? k)
    (throw (IllegalArgumentException.
             (format "Invalid key present (%s)." (pr-str k)))))
  (when-not (map? v)
    (throw (IllegalArgumentException.
             (format "Invalid value present (%s)." (type v)))))
  [k (entry k v 0)])

(def ^:private always-nil
  (constantly nil))

(def ^:private always-false
  (constantly false))


(extend nil
  proto/KVDBEntry
  {:entry?   always-false
   :key      always-nil
   :value    always-nil
   :revision always-nil}
  proto/ReadableKVDB
  {:readable-kvdb? always-false}
  proto/MutableKVDB
  {:mutable-kvdb? always-false}
  proto/PageableKVDB
  {:pageable-kvdb? always-false}
  proto/OverridableKVDB
  {:overridable-kvdb? always-false})


(extend Object
  proto/KVDBEntry
  {:entry?   always-false
   :key      always-nil
   :value    always-nil
   :revision always-nil}
  proto/ReadableKVDB
  {:readable-kvdb? always-false}
  proto/MutableKVDB
  {:mutable-kvdb? always-false}
  proto/PageableKVDB
  {:pageable-kvdb? always-false}
  proto/OverridableKVDB
  {:overridable-kvdb? always-false})


(extend-type clojure.lang.IPersistentMap
  proto/KVDBEntry
  (entry? [x]
    (identical? ::entry (type x)))
  (key [e]
    (::key (meta e)))
  (revision [e]
    (::revision (meta e) 0))
  (value [e]
    (when (proto/entry? e)
      e))

  proto/KVDBable
  (to-kvdb [m]
    (-> (into (empty m) (map enrich-map-entry (seq m)))
        (vary-meta assoc ::readable-kvdb true)))

  proto/ReadableKVDB
  (readable-kvdb? [x]
    (true? (::readable-kvdb (meta x))))
  (fetch [m k missing-v]
    (if-some [e (find m k)]
      (val e)
      missing-v))
  (entries [m]
    (or (vals m))))


(extend-type clojure.lang.PersistentTreeMap
  proto/KVDBable
  (to-kvdb [m]
    (-> (into (empty m) (map enrich-map-entry (seq m)))
        (vary-meta assoc ::readable-kvdb true
                         ::pageable-kvdb true)))

  proto/PageableKVDB
  (pageable-kvdb? [x]
    (true? (::pageable-kvdb (meta x))))
  (page [m exclusive-start-key limit]
    (take (or limit 50)
          (if exclusive-start-key
            (map core/val
                 (subseq m > exclusive-start-key))
            (vals m)))))


(defn merge-overridden-methods
  [metadata method-impls]
  (merge metadata
         method-impls
         {::overrides-stack
          (cons method-impls
                (::overrides-stack metadata ))}))


(defn pop-overridden-methods
  [metadata]
  (let [overrides-stack (::overrides-stack metadata [])]
    (merge (reduce dissoc metadata (keys (first overrides-stack)))
           (second overrides-stack)
           {::overrides-stack (next overrides-stack)}
           )))

(extend-type clojure.lang.IObj
  proto/OverridableKVDB
  (overridable-kvdb? [x]
    (proto/readable-kvdb? x))
  (push-overrides [kvdb implementations]
    (vary-meta kvdb merge-overridden-methods implementations))
  (overridden [kvdb]
    (select-overridable-methods (meta kvdb)))
  (pop-overrides [kvdb]
    (vary-meta kvdb pop-overridden-methods)))


(s/fdef resolve-protocol
  :args (s/cat :symbol qualified-symbol?)
  :ret  (s/or :nothing nil?
              :var     var?))

(defn ^:private resolve-protocol
  [sym]
  (when (symbol? sym)
    (:protocol (meta (resolve sym)))))


(def ^:private direct-methods
  {`proto/fetch    (memfn ^ReadableKVDB fetch kvdb k missing-value)
   `proto/entries  (memfn ^ReadableKVDB entries kvdb)
   `proto/create!  (memfn ^MutableKVDB create! kvdb key value)
   `proto/remove!  (memfn ^MutableKVDB remove! kvdb key revision)
   `proto/replace! (memfn ^MutableKVDB replace! kvdb key revision new-value)
   `proto/page     (memfn ^PageableKVDB page kvdb starting-key limit)
    })


(s/fdef find-implementation
  :args (s/cat :kvdb kvdb?
               :protocol-method ::overridable-kvdb-method)
  :ret  (s/or :nothing nil?
              :impl    fn?))

(defn find-implementation
  "Find the implementation of a protocol method for an object."
  [kvdb protomethod]
  (when-some [proto (resolve-protocol protomethod)]
    (or (get (meta kvdb) protomethod)
        (let [iface (:on-interface @proto)
              method-name (name protomethod)]
          (if (instance? iface kvdb)
            (get direct-methods protomethod)
            (find-protocol-method @proto (keyword method-name) kvdb))))))


(defn coerced
  "Construct a coerced KVDB.
Values will be transformed with the coerce function
after being read from the underlying KVDB, and by the
uncoerce function when before being written to the KVDB."
  ([kvdb {:keys [coerce uncoerce]}]
   (coerced kvdb (or coerce identity)
                 (or uncoerce identity)))
  ([kvdb coerce uncoerce]
   (if-not (overridable-kvdb? kvdb)
     (throw (ex-info "Can only coerce an OverridableKVDB."
                     {:kvdb kvdb
                      :coerce coerce
                      :uncoerce uncoerce}))
     (let [coerce (partial fmap-entry coerce)]
       (override kvdb
         (merge
           (when (readable-kvdb? kvdb)
             {`proto/fetch   (let [f (find-implementation kvdb `proto/fetch)]
                               (fn [kvdb k missing-value]
                                 (let [v (f kvdb k missing-value)]
                                   (if (identical? missing-value v)
                                     missing-value
                                     (coerce v)))))
              `proto/entries (comp (partial map coerce)
                                   (find-implementation kvdb `proto/entries))
              })
           (when (mutable-kvdb? kvdb)
             {`proto/create!  (let [c! (find-implementation kvdb `proto/create!)]
                                (fn [kvdb key value]
                                  (coerce
                                    (c! kvdb key (uncoerce value)))))
              `proto/remove!  (comp coerce
                                    (find-implementation kvdb `proto/remove!))
              `proto/replace! (let [r! (find-implementation kvdb `proto/replace!)]
                                (fn [kvdb key revision new-value]
                                  (coerce
                                    (r! kvdb key revision (uncoerce new-value)))))
              })
           (when (pageable-kvdb? kvdb)
             {`proto/page (comp (partial map coerce)
                                (find-implementation kvdb `proto/page))
              })))))))

