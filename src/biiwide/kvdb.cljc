(ns biiwide.kvdb
  (:refer-clojure :exclude [key remove replace])
  (:require [biiwide.kvdb.protocols :as proto]
            [clojure.core :as core]
            [#?(:clj  clojure.pprint
                :cljs cljs.pprint) :as pprint
              :refer [cl-format]]
            [clojure.spec.alpha :as s])
  #?(:clj (:import (clojure.lang ExceptionInfo))))


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

(defn illegal-argument
  "Construct an exception for unusable arguments."
  [message & params]
  (ex-info (apply cl-format nil message params)
           {:type ::illegal-argument}))

(defn illegal-argument-exception?
  [x]
  (= ::illegal-argument (:type (ex-data x))))


(defn to-kvdb
  "Coerce a potential KVDB value into a full KVDB."
  [x]
  (cond (kvdb? x)     x
        (kvdbable? x) (proto/to-kvdb x)
        :else
        (throw (illegal-argument "Cannot create a KVDB from ~s"
                                 (type x)))))


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
  :ret  (s/every ::entry))

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


(s/fdef key-collision-exception?
  :args (s/cat :any any?)
  :ret  boolean?)

(defn key-collision-exception?
  "Test for a key-collision exception."
  [ex]
  (= ::key-collision
     (:type (ex-data ex))))

(s/def ::action
  #{::create! ::replace! ::remove!})

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
  [kvdb action key actual-rev expected-rev]
  (ex-info "Actual revision did not match expected revision."
           {:type              ::revision-mismatch
            :kvdb              kvdb
            :action            action
            :key               key
            :actual-revision   actual-rev
            :expected-revision expected-rev}))


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
  [kvdb action key]
  (ex-info "Key not found in database."
           {:type   ::key-not-found
            :kvdb   kvdb
            :action action
            :key    key}))


(defn ^:private exception?
  [x]
  #?(:clj (instance? Exception x)
     :cljs (instance? js/Error x)))


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
  [kvdb key f attempts & [cause]]
  (ex-info "Exceeded maximum transact attempts."
           {:type     ::exceeded-transact-attempts
            :kvdb     kvdb
            :action   ::transact-values!
            :key      key
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
  [kvdb key f]
  (let [missing-v #?(:clj (Object.) :cljs (js-obj))
        ent (proto/fetch kvdb key missing-v)
        missing? (identical? missing-v ent)
        old-val (if missing? nil (value ent))
        new-val (f old-val)]
    (cond missing?
          (if (removal? new-val)
            [nil nil]
            (try [nil (create! kvdb key new-val)]
              (catch ExceptionInfo ex
                (cond (key-collision-exception? ex) ex
                      :else (throw ex)))))
          (removal? new-val)
          (try [(remove! kvdb key (revision ent)) nil]
            (catch ExceptionInfo ex
              (cond (revision-mismatch-exception? ex) ex
                    (key-not-found-exception? ex) ex
                    :else (throw ex))))
          (= old-val new-val)
          [old-val old-val]
          :else
          (try [ent (replace! kvdb key (revision ent) new-val)]
            (catch ExceptionInfo ex
              (cond (revision-mismatch-exception? ex) ex
                    (key-not-found-exception? ex) ex
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
  "Atomically alter the value of an entry, similar to clojure.core/swap!.
The value for a key will be read, passed to the provided function, and
saved back to the database.
* When the entry is modified conconcurrently, the process will be repeated
until either success or the maximum number of attempts has been reached.
* Transacting an entry that does not exist will create a new entry.
* If the value is not modified by the provided function, then no action will
be taken.
* The kvdb/removal function can be used to remove an entry from a transacting
function."
  ([kvdb key f a]
   (transact-values! kvdb key #(f % a)))
  ([kvdb key f a b]
   (transact-values! kvdb key #(f % a b)))
  ([kvdb key f a b c]
   (transact-values! kvdb key #(f % a b c)))
  ([kvdb key f a b c & more-args]
   (transact-values! kvdb key #(apply f % a b c more-args)))
  ([kvdb key f]
   (let [max-attempts *max-transact-attempts*
         rand-pause (fn [] #?(:clj (Thread/sleep 0 (rand-int 999)))
                              :cljs (do (max (range (rand-int 9999))) nil))]
     (loop [remaining-attempts max-attempts
            last-exception     nil]
       (if (pos? remaining-attempts)
         (let [result (apply-tx kvdb key f)]
           (if (exception? result)
             (do (rand-pause)
                 (recur (dec remaining-attempts) result))
             result))
         (throw
           (exceeded-transact-attempts
             kvdb key f max-attempts last-exception)))))))


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
  (comp second transact-values!))


(defmacro with-max-transact-attempts
  "Override the default maximum transaction attempts within a scope."
  [max-attempts & body]
  (let [check (cond (number? max-attempts)
                    (assert (pos? max-attempts)
                            "Attempts must be a positive integer")
                    (symbol? max-attempts)
                    `((assert (pos? ~max-attempts)
                              "Attempts must be a positive integer"))
                    :else (throw (illegal-argument "Invalid max-attempts ~s"
                                                   max-attempts)))]
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
  [x]
  (and (readable-kvdb? x)
       (proto/overridable-kvdb? x)))

(s/def ::overridable-kvdb overridable-kvdb?)


(s/def ::readable-kvdb-method
  #{`proto/fetch
    `proto/entries})

(s/def ::mutable-kvdb-method
  #{`proto/create!
    `proto/remove!
    `proto/replace!})

(s/def ::pageable-kvdb-method
  #{`proto/page})

(s/def ::protocol-method
  (s/or :readable-kvdb-method ::readable-kvdb-method
        :mutable-kvdb-method  ::mutable-kvdb-method
        :pageable-kvdb-method ::pageable-kvdb-method))


(s/fdef override
  :args (s/cat :kvdb ::overridable-kvdb
               :impl (s/alt :single (s/cat :method ::protocol-method
                                           :impl   ifn?)
                            :multiple (s/map-of ::protocol-method ifn?)))
  :ret  ::overridable-kvdb)

(defn override
  ([kvdb protocol-method implementation]
   (override kvdb {protocol-method implementation}))
  ([kvdb implementations]
   (proto/override kvdb implementations)))


(defn ^:private enrich-map-entry
  [[k v]]
  (when-not (string? k)
    (throw (illegal-argument "Invalid key present (~s)." k)))
  (when-not (map? v)
    (throw (illegal-argument "Invalid value present (~s)."
                             (type v))))
  [k (entry k v 0)])


(extend-type nil
  proto/KVDBEntry
  (entry? [_] false)
  (key [_] nil)
  (value [_] nil)
  (revision [_] nil)
  proto/ReadableKVDB
  (readable-kvdb? [_] false)
  proto/MutableKVDB
  (mutable-kvdb? [_] false)
  proto/PageableKVDB
  (pageable-kvdb? [_] false)
  proto/OverridableKVDB
  (overridable-kvdb? [_] false))

(extend-type
  #?(:clj Object :cljs default)
  proto/KVDBEntry
  (entry? [_] false)
  (key [_] nil)
  (value [_] nil)
  (revision [_] nil)
  proto/ReadableKVDB
  (readable-kvdb? [_] false)
  proto/MutableKVDB
  (mutable-kvdb? [_] false)
  proto/PageableKVDB
  (pageable-kvdb? [_] false)
  proto/OverridableKVDB
  (overridable-kvdb? [_] false))


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
  (fetch [m key missing-val]
    (if-some [e (find m key)]
      (val e)
      missing-val))
  (entries [m]
    (vals m)))


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


(extend-type clojure.lang.IObj
  proto/OverridableKVDB
  (overridable-kvdb? [x]
    (proto/readable-kvdb? x))
  (override [kvdb implementations]
    (vary-meta kvdb merge implementations)))
