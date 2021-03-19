(ns biiwide.kvdb.proto.verification
  "This Namespace provides property generators for verifying
compliance with the KVDB protocol.

Usage:
```
(defspec my-custom-kvdb
  (verification/kvdb-properties my-kvdb-generator))
```"
  (:require [biiwide.kvdb :as kvdb]
            [biiwide.kvdb.protocols :as proto]
            [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as stest]
            [clojure.string :as string]
            [clojure.test :refer [is]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop
             :refer [for-all]]))


(defn instrumentable-kvdb-syms []
  (filter (fn [sym]
            (string/starts-with? (namespace sym) "biiwide.kvdb"))
          (stest/instrumentable-syms)))

(defn ^:private as-seq [x]
  (if (sequential? x) x [x]))

(defn ^:private fnspec? [x]
  (or (:args x) (:ret x)))

(def ^:private ->sym @#'s/->sym)

(defn ^:private checked-fn?
  [f]
  (boolean (::checked-fn (meta f))))

(defn ^:private unchecked-fn
  [f]
  (::checked-fn (meta f) f))

(def ^:dynamic *instrumentation-enabled* true)

(defmacro without-instrumentation
  [& body]
  `(binding [*instrumentation-enabled* false]
     ~@body))

(defn ^:private checked-fn
  [f fsym fnspec]
  (cond (not (fnspec? fnspec)) f
        (checked-fn? f) (recur (unchecked-fn f) fsym fnspec)
        :else
        (letfn [(conform! [value role phase spec args & [ret]]
                  (when *instrumentation-enabled*
                    (let [conformed (when spec (s/conform spec value))]
                      (if (= ::s/invalid conformed)
                        (throw (ex-info
                                 (format "%s %s did not conform to spec\n  %s"
                                         phase fsym (s/explain-str spec value))
                                 (-> (assoc (s/explain-data* spec [] [] [] value)
                                            ::s/spec (s/describe spec)
                                            ::s/fn fsym
                                            ::s/args args
                                            ::s/role role
                                            ::s/failure :instrument)
                                     (cond-> ret (assoc ::s/ret ret)))))
                        conformed))))]
          (let [fn-spec (when (and (:args fnspec) (:ret fnspec))
                          (:fn fnspec))]
            (-> (fn [& args]
                  (let [con-args (conform! args :args "Call to" (:args fnspec) args)
                        ret      (apply f args)
                        con-ret  (conform! ret :ret "Result of" (:ret fnspec) args ret)]
                    (when fn-spec
                      (conform! {:args con-args :ret con-ret}
                                 :fn "Result of" fn-spec args ret))
                    ret))
                (vary-meta assoc ::checked-fn f))))))

(defn instrument-var!
  "Similar to clojure.spec.test.alpha/instrument! but conforms and validates
both input and output using :args, :ret, & :fn specs."
  [v]
  {:pre [(var? v)]}
  (alter-var-root v checked-fn (->sym v) (s/get-spec (->sym v)))
  v)

(defn instrument-kvdb!
  "Similar to clojure.spec.test.alpha/instrument! but conforms and validates
both input and output using :args, :ret, & :fn specs."
  ([]
    (instrument-kvdb! (instrumentable-kvdb-syms)))
  ([sym-or-syms]
    (vec (for [sym (as-seq sym-or-syms)]
           (instrument-var! (resolve sym))))))

(defn unstrument-var!
  [v]
  (alter-var-root v unchecked-fn)
  v)

(defn unstrument-kvdb!
  "Removes spec instrumentation installed by instrument-kvdb!"
  ([]
    (unstrument-kvdb! (instrumentable-kvdb-syms)))
  ([sym-or-syms]
    (doseq [sym (as-seq sym-or-syms)]
      (unstrument-var! (resolve sym)))))

(instrument-kvdb!)


(defmacro and-let
  "A macro for mixing bindings and tests.

(and-let
  [a (do something)]
  (is (number? a))
  [b (do something else)]
  (is (string? b)))"
  [& checks-and-bindings]
  (cond (empty? checks-and-bindings) true
        (vector? (first checks-and-bindings))
        (let [[bindings & more] checks-and-bindings]
          `(let ~bindings (and-let ~@more )))
        :else
        (let [[exprs more] (split-with (complement vector?)
                                       checks-and-bindings)]
          `(and ~@exprs (and-let ~@more)))))


(defn valid-entry?
  [x]
  (and (is (kvdb/entry? x))
       (is (s/valid? ::kvdb/key (kvdb/key x)))
       (is (s/valid? ::kvdb/value (kvdb/value x)))
       (is (s/valid? ::kvdb/revision (kvdb/revision x)))))


(defn ^:private lazyseq? [x]
  (instance? clojure.lang.LazySeq x))


(defmacro property
  [property-name context-args generator-bindings body]
  (let [generated-syms (map first (partition 2 generator-bindings))]
    `(-> (gen/let ~generator-bindings
           (fn [{:as ctxt# :keys ~context-args}]
             (try (let [result# ~body]
                    {:result   result#
                     :function (quote ~property-name)
                     :context  ctxt#
                     :args     {~(keyword property-name)
                                (zipmap ~(map keyword generated-syms)
                                        ~generated-syms)}})
               (catch Exception e#
                 (prop/->ErrorResult e#)))))
          (vary-meta assoc ::property (quote ~property-name)))))

(defmacro defproperty
  [property-name doc context-args generator-bindings body]
  `(def ~property-name ~doc
     (property ~property-name ~context-args ~generator-bindings ~body)))


(defn guard-property [p? property]
  (gen/fmap (fn [prop-fn]
              (fn [context]
                (if (p? context)
                  (prop-fn context)
                  {:result   true
                   :function (::property (meta property))
                   :context  context
                   :args     {(::property (meta property)) {}}})))
            property))
  

(defproperty readable-kvdb-properties
  "A test for ReadableKVDB properties that can be bound to a KVDB generator."
  [kvdb]
  [missing-k (gen/fmap str gen/uuid)
   missing-v gen/any]
  (if-not (kvdb/readable-kvdb? kvdb)
    true
    (and
      (is (kvdb/readable-kvdb? kvdb)
          "The generated database is a ReadableKVDB")
      (is (identical? missing-v
                      (kvdb/fetch kvdb missing-k missing-v))
          "Fetching a missing key returns the provided missing-value")
      (every? (fn [entry]
                (and (valid-entry? entry)
                     (is (= entry (kvdb/fetch kvdb (kvdb/key entry)))
                         "Every entry from entries can be fetched")))
              (kvdb/entries kvdb)))))


(defproperty pageable-kvdb-properties
  "A test for PaginagedKVDB properties that can be bound to a KVDB generator."
  [kvdb]
  [random-key gen/string
   limit      (gen/large-integer* {:min 1 :max 2000})]
  (if-not (kvdb/pageable-kvdb? kvdb)
    true
    (and-let
      [all-entries (kvdb/entries kvdb)
       random-page (kvdb/page kvdb random-key limit)]
      (is (kvdb/pageable-kvdb? kvdb)
          "The generated database is a PageableKVDB")
      (is (<= (count random-page) limit)
          "Requesting a page returns no more entries than the limit")
      (is (<= (count random-page)
              (count all-entries))
          (str "Requesting a page returns no more entries than exist "
               " in the entire KVDB"))
      (is (lazyseq? (kvdb/pageseq kvdb)))
      (is (= (group-by kvdb/key all-entries)
             (group-by kvdb/key (kvdb/pageseq kvdb)))
          "pageseq is equivalent to entries")
      (every? (fn [entry]
                (and (valid-entry? entry)
                     (is (not= random-key (kvdb/key entry))
                         "No pageseq entries match the exclusive-start-key.")))
              (kvdb/pageseq kvdb random-key limit)))))


(defn mutable-kvdb-properties
  "A test for MutableKVDB properties that can be bound to a KVDB generator."
  [value-generator]
  (property mutable-kvdb-properties
    [kvdb]
    [new-key     gen/string-ascii
     value-one   value-generator
     value-two   value-generator
     counter     (gen/large-integer* {:min 1 :max 50})]
    (if-not (kvdb/mutable-kvdb? kvdb)
      true
      (and-let
        [_ (kvdb/transact! kvdb new-key kvdb/removal)]
        (is (kvdb/mutable-kvdb? kvdb)
            "The generated database is a MutableKVDB.")
        (is (nil? (kvdb/fetch kvdb new-key))
            (format "The new key \"%s\" is not present." new-key))

        (is (kvdb/key-not-found-exception?
              (is (thrown? Exception (kvdb/replace! kvdb new-key 99 value-one))))
            "A key-not-found exception is thrown when replacing a missing key.")

        (is (kvdb/key-not-found-exception?
              (is (thrown? Exception (kvdb/remove! kvdb new-key 99))))
            "A key-not-found exception is thrown when removing a missing key.")

        [created (kvdb/create! kvdb new-key value-one)]
        (is (kvdb/entry? created)
            "Creating an entry returns the new entry.")
        (is (kvdb/key-collision-exception?
              (is (thrown? Exception (kvdb/create! kvdb new-key value-one))))
            "A key-collision exception is thrown when creating an existing key.")

        [revision-one (kvdb/revision created)]
        (is (kvdb/revision-mismatch-exception?
              (is (thrown? Exception
                    (kvdb/replace! kvdb new-key (inc revision-one) value-two))
                  "Replacing an entry using the wrong revision throws an exception."))
            "A revision-mismatch exception is thrown.")
        (is (kvdb/revision-mismatch-exception?
              (is (thrown? Exception
                    (kvdb/replace! kvdb new-key (+ counter revision-one) value-two))
                  "Replacing an entry using the wrong revision throws an exception."))
            "A revision-mismatch exception is thrown.")

        [replaced (kvdb/replace! kvdb new-key revision-one value-two)]
        (is (kvdb/entry? replaced)
            "Replace! returns a valid entry.")
        (is (not= (kvdb/revision created)
                  (kvdb/revision replaced))
            "The revision changes when an entry is replaced.")
        (is (= value-two (kvdb/value replaced))
            "Replace! returns the new value.")

        [revision-two (kvdb/revision replaced)]
        (is (kvdb/revision-mismatch-exception?
              (is (thrown? Exception (kvdb/remove! kvdb new-key revision-one))
                  "Replacing an entry using the wrong revision throws an exception."))
            "A revision-mismatch exception is thrown.")

        [removed (kvdb/remove! kvdb new-key revision-two)]
        (is (kvdb/entry? removed)
            "Removing an entry returns the removed entry.")
        (is (= revision-two (kvdb/revision removed))
            "The revision of the removed entry matches the last revision.")
        (is (= value-two (kvdb/value removed))
            "The value of the removed entry matches the last value")

        (is (nil? (kvdb/fetch kvdb new-key))
            (format "The removed key \"%s\" is no longer present."
                    new-key))
        ))))


(defn transact-kvdb-properties
  "Properties of kvdb/transact! for MutableKVDB instances."
  [value-generator]
  (letfn [(all-nil? [m] (every? nil? (vals m)))
          (tx [v new-v] (cond (nil? v)     {}
                              (all-nil? v) new-v
                              :else        (kvdb/removal v)))]
    (property transact-kvdb-properties
      [kvdb]
      [new-key gen/string-ascii
       new-val (gen/such-that #(not (all-nil? %))
                              value-generator)
       third (gen/large-integer* {:min 1 :max 50})]
      (if-not (kvdb/mutable-kvdb? kvdb)
        true
        (and-let
          [_          (kvdb/transact! kvdb new-key kvdb/removal)
           tx-entries (doall (pmap (partial kvdb/transact! kvdb new-key tx)
                                   (repeat (* 3 third) new-val)))
           tx-values  (map kvdb/value tx-entries)]
          (is (= third (count (filter nil? tx-values)))
              "One third of results are removals.")
          (is (= third (count (filter all-nil? (remove nil? tx-values))))
              "One third of results are new entries.")
          (is (= third (count (remove all-nil? (remove nil? tx-values))))
              "One third of results are updated entries.")

          (is (nil? (kvdb/fetch kvdb new-key))
              "The entry is absent when all transactions have completed.")
          )))))


(def ^:private gen-overridable-method
  (gen/elements [`proto/fetch `proto/entries
                 `proto/create! `proto/remove!
                 `proto/replace! `proto/page]))

(defn ^:private overridable?
  [kvdb method result f & args]
  (identical? result
              (apply f (kvdb/override kvdb method (constantly result)) args)))

(defproperty overridable-kvdb-properties
  "Test properties of Overridable KVDBs"
  [kvdb]
  [k gen/string
   v (gen/map gen/string-ascii gen/large-integer)
   r (gen/large-integer* {:min 1})
   methods (gen/set gen-overridable-method {:min 1 :max 6})]
  (if-not (kvdb/overridable-kvdb? kvdb)
    true
    (without-instrumentation
      (and-let [token  (kvdb/entry k v r)
                tokens [token token]]
        (if (kvdb/readable-kvdb? kvdb)
          (and (is (overridable? kvdb `proto/fetch token kvdb/fetch "aaa")
                   "Can override ReadableKVDB/fetch")
               (is (overridable? kvdb `proto/entries tokens kvdb/entries)
                   "Can override ReadableKVDB/entries"))
          true)
        (if (kvdb/mutable-kvdb? kvdb)
          (and (is (overridable? kvdb `proto/create! token kvdb/create! "bbb" {})
                   "Can override MutableKVDB/create!")
               (is (overridable? kvdb `proto/remove! token kvdb/remove! "CCC" 4)
                   "Can override MutableKVDB/remove!")
               (is (overridable? kvdb `proto/replace! token kvdb/replace! "ddd" 1 {})
                   "Can override MutableKVDB/replace!"))
          true)
        (if (kvdb/pageable-kvdb? kvdb)
          (is (overridable? kvdb `proto/page tokens kvdb/page "" 1)
              "Can override PageableKVDB/page")
          true)

        [method-impls (zipmap methods (repeat (constantly nil)))]
        (is (= (merge (kvdb/overridden kvdb)
                      method-impls)
               (kvdb/overridden (kvdb/override kvdb method-impls))))
        (is (= (kvdb/overridden kvdb)
               (kvdb/overridden
                 (kvdb/pop-overrides (kvdb/override kvdb method-impls)))))
        ))))


(def kvdb-value-generator
  (gen/map gen/simple-type-printable-equatable
           gen/any-printable-equatable))


(defn kvdb-generator
  [gen-make-kvdb gen-key gen-value]
  (let [gen->kvdb (if (gen/generator? gen-make-kvdb)
                    gen-make-kvdb
                    (gen/return gen-make-kvdb))]
    (gen/let [->kvdb gen->kvdb
              data   (gen/map gen-key gen-value)]
      (->kvdb data))))


;(defn kvdb-properties
;  "A test for KVDB properties that can be bound to a KVDB generator.
;This will identify the protocols implemented for a KVDB instance and
;perform all tests for the supported protocols."
;  ([gen-kvdb-context]
;   (kvdb-properties gen-kvdb-context nil))
;  ([gen-kvdb-context value-generator]
;   (let [always-true (gen/return true)
;         value-generator (or value-generator kvdb-value-generator)]
;     (gen/fmap (fn [ctxt args]
;                 (ctxt values)).
;     (with-kvdb-context gen-kvdb-context
;       (fn [kvdb]

(defn within-context
  [gen-context gen-property & more-properties]
  (gen/let [ctxt gen-context
            props (apply gen/tuple gen-property more-properties)]
    (map ctxt props)))


(defn all-kvdb-properties
  "A test for KVDB properties that can be bound to a KVDB generator.
This will identify the protocols implemented for a KVDB instance and
perform all tests for the supported protocols."
  ([gen-kvdb-context]
   (all-kvdb-properties gen-kvdb-context nil))
  ([gen-kvdb-context value-generator]
   (let [value-generator (or value-generator kvdb-value-generator)]
     (within-context gen-kvdb-context
       readable-kvdb-properties
       pageable-kvdb-properties
       (mutable-kvdb-properties value-generator)
       (transact-kvdb-properties value-generator)
       overridable-kvdb-properties))))

(defspec check-persistent-map-properties
  (all-kvdb-properties
    (gen/let [entries (gen/hash-map gen/string kvdb-value-generator)]
      (fn [property]
        (property {:kvdb (kvdb/to-kvdb entries)})))))


#_(defn all-kvdb-properties
   "A test for KVDB properties that can be bound to a KVDB generator.
This will identify the protocols implemented for a KVDB instance and
perform all tests for the supported protocols."
   ([gen-make-kvdb]
    (kvdb-properties gen-make-kvdb nil))
   ([gen-make-kvdb value-generator]
    (let [always-true (gen/return true)
          value-generator (or value-generator kvdb-value-generator)]
      (gen/bind
        (kvdb-generator gen-make-kvdb gen/string value-generator)
        (fn [kvdb]
          (for-all [readable?    (readable-kvdb-properties kvdb)
                    pageable?    (if (kvdb/pageable-kvdb? kvdb)
                                   (pageable-kvdb-properties kvdb)
                                   always-true)
                    mutable?     (if (kvdb/mutable-kvdb? kvdb)
                                   (if (nil? value-generator)
                                     (throw (ex-info "Value generator is required" {}))
                                     (mutable-kvdb-properties kvdb value-generator))
                                   always-true)
                    transact?    (if (kvdb/mutable-kvdb? kvdb)
                                   (if (nil? value-generator)
                                     (throw (ex-info "Value generator is required" {}))
                                     (transact-kvdb-properties kvdb value-generator))
                                   always-true)
                    overridable? (if (kvdb/overridable-kvdb? kvdb)
                                   (overridable-kvdb-properties kvdb)
                                   always-true)]
            (boolean
              (and (is readable?    "ReadableKVDB Property Failure")
                   (is pageable?    "PageableKVDB Property Failure")
                   (is mutable?     "MutableKVDB Property Failure")
                   (is transact?    "MutableKVDB Transact Failure")
                   (is overridable? "OverridableKVDB Propery Failure")
                   ))))))))
