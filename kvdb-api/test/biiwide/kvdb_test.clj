(ns biiwide.kvdb-test
  (:require [biiwide.kvdb :as kvdb
             :refer [to-kvdb]]
            [biiwide.kvdb.atomic :as a]
            [biiwide.kvdb.generators :as kgen]
            [biiwide.kvdb.protocols :as proto]
            [biiwide.kvdb.proto.verification :as v
            :refer [and-let instrument-kvdb! kvdb-properties]]
            [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as stest]
            [clojure.test :refer :all]
            [clojure.test.check.clojure-test
             :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]))


(instrument-kvdb!)


(deftest test-entry?
  (are [p? value]
    (p? (kvdb/entry? value))

    false? nil
    false? ""
    false? {}
    true?  (kvdb/entry "K" {} 0)
    true?  (kvdb/entry "L" {:a 1} 444)
    ))


(deftest test-entry-invalid
  (stest/instrument [#'kvdb/entry])
  (are [k v rev]
    (is (thrown? Exception
          (kvdb/entry k v rev)))

    :k  {}  0
    1   {}  0
    "a" "v" 0
    "b" 12  0
    "c" []  0
    "d" {}  "a"
    "e" {}  -1
    ))

(deftest test-key
  (are [expected input]
    (= expected (kvdb/key input))

    nil   nil
    nil   true
    nil   0
    nil   ""
    nil   []
    nil   {}
    "key" (kvdb/entry "key" {} 0)
    ))


(deftest test-value
  (are [expected input]
    (= expected (kvdb/value input))

    nil   nil
    nil   true
    nil   0
    nil   ""
    nil   []
    nil   {}
    {}    (kvdb/entry "val" {} 1)
    ))


(deftest test-revision
  (are [expected input]
    (= expected (kvdb/revision input))

    nil   nil
    nil   true
    nil   0
    nil   ""
    nil   []
    nil   {}
    321   (kvdb/entry "rev" {} 321)
    ))


(deftest test-readable-kvdb?
  (are [p? v]
    (p? (kvdb/readable-kvdb? v))

    false? nil
    false? ""
    false? 123
    false? []
    false? {}
    false? (sorted-map)
    false? (atom {})
    true?  (to-kvdb {})
    true?  (to-kvdb (sorted-map))
    true?  (to-kvdb (atom {}))
    ))


(deftest test-pageable-kvdb?
  (are [p? v]
    (p? (kvdb/pageable-kvdb? v))

    false? nil
    false? ""
    false? 123
    false? []
    false? {}
    false? (sorted-map)
    false? (atom {})
    false? (to-kvdb {})
    false? (to-kvdb (atom {}))
    true?  (to-kvdb (sorted-map))
    true?  (to-kvdb (atom (sorted-map)))
    ))


(deftest test-mutable-kvdb?
  (are [p? v]
    (p? (kvdb/mutable-kvdb? v))

    false? nil
    false? ""
    false? 123
    false? []
    false? {}
    false? (sorted-map)
    false? (atom {})
    false? (to-kvdb {})
    true?  (to-kvdb (atom {}))
    false? (to-kvdb (sorted-map))
    true?  (to-kvdb (atom (sorted-map)))
    ))


(deftest test-overridable-kvdb?
  (are [p? v]
    (p? (kvdb/overridable-kvdb? v))

    false? nil
    false? ""
    false? 123
    false? []
    false? {}
    false? (sorted-map)
    false? (atom {})
    true?  (to-kvdb {})
    true?  (to-kvdb (atom {}))
    true?  (to-kvdb (sorted-map))
    true?  (to-kvdb (atom (sorted-map)))
    ))


(deftest test-invalid-kvdb-source-value
  (are [m ex-class ex-message]
    (is (thrown-with-msg? ex-class ex-message
          (kvdb/to-kvdb m)))

    nil                      RuntimeException          #"(?i)\bCannot create a KVDB\b"
    true                     RuntimeException          #"(?i)\bCannot create a KVDB\b"
    5555                     RuntimeException          #"(?i)\bCannot create a KVDB\b"
    "{\"abc\" 1}"            RuntimeException          #"(?i)\bCannot create a KVDB\b"
    []                       RuntimeException          #"(?i)\bCannot create a KVDB\b"

    {nil {}}                 IllegalArgumentException  #"(?i)\bInvalid key\b"
    {:one {}}                IllegalArgumentException  #"(?i)\bInvalid key\b"
    {1234 {}}                IllegalArgumentException  #"(?i)\bInvalid key\b"
    {"abc" nil}              IllegalArgumentException  #"(?i)\bInvalid value\b"
    {"def" 111}              IllegalArgumentException  #"(?i)\bInvalid value\b"
    {"ghi" "b"}              IllegalArgumentException  #"(?i)\bInvalid value\b"

    (sorted-map nil {})      IllegalArgumentException  #"(?i)\bInvalid key\b"
    (sorted-map true {})     IllegalArgumentException  #"(?i)\bInvalid key\b"
    (sorted-map 123 {})      IllegalArgumentException  #"(?i)\bInvalid key\b"
    (sorted-map :two {})     IllegalArgumentException  #"(?i)\bInvalid key\b"
    (sorted-map "111" nil)   IllegalArgumentException  #"(?i)\bInvalid value\b"
    (sorted-map "222" true)  IllegalArgumentException  #"(?i)\bInvalid value\b"
    (sorted-map "333" 444)   IllegalArgumentException  #"(?i)\bInvalid value\b"
    (sorted-map "444" "abc") IllegalArgumentException  #"(?i)\bInvalid value\b"
    ))


(defspec transact-values-new-entry-spec 50
  (prop/for-all [new-k gen/string
                 new-v kgen/value]
    (and-let
      [db (kvdb/to-kvdb (atom {}))]
      (is (thrown-with-msg? Exception #"Boom!"
            (kvdb/transact-values! db new-k
              (fn [v] (when (nil? v)
                        (throw (Exception. "Boom!"))))))
          "Unexpected exceptions are rethrown")

      [result (apply kvdb/transact-values!
                     db new-k
                     (fn [orig-v & pairs]
                       (assert (nil? orig-v))
                       (into {} pairs))
                     (seq new-v))]
      (is (vector? result))
      (is (= 2 (count result)))

      [[old-entry new-entry] result]
      (is (nil? old-entry))

      (is (kvdb/entry? new-entry))
      (is (= new-k (kvdb/key new-entry)))
      (is (= new-v (kvdb/value new-entry)))
      )))


(defspec transact-values-update-entry-spec 50
  (prop/for-all [k gen/string
                 v kgen/value
                 field-k gen/keyword
                 field-v gen/simple-type-printable-equatable]
    (and-let
      [db (kvdb/to-kvdb (atom {k v}))
       result (kvdb/transact-values! db k assoc field-k field-v)]
      (is (vector? result))
      (is (= 2 (count result)))

      [[old-entry new-entry] result]
      (is (= k (kvdb/key old-entry)))
      (is (= v (kvdb/value old-entry)))

      (is (= k (kvdb/key new-entry)))
      (is (= (assoc v field-k field-v)
             (kvdb/value new-entry)))

      (is (not= (kvdb/revision old-entry)
                (kvdb/revision new-entry)))

      [[old-e new-e] (kvdb/transact-values! db k identity)]
      (is (= (kvdb/revision old-e)
             (kvdb/revision new-e)))
      )))


(defspec transact-values-remove-entry-spec 50
  (prop/for-all [k gen/string
                 v kgen/value]
    (and-let
      [db (kvdb/to-kvdb (atom {}))
       remove-missing (kvdb/transact-values! db k kvdb/removal)]
      (is (vector? remove-missing))
      (is (= [nil nil] remove-missing))

      [created (kvdb/create! db k v)
       remove-result (kvdb/transact-values! db k kvdb/removal)]
      (is (vector? remove-result))
      (is (= 2 (count remove-result)))

      [[old-entry new-entry] remove-result]
      (is (kvdb/entry? old-entry))
      (is (= k (kvdb/key old-entry)))
      (is (= (kvdb/revision created)
             (kvdb/revision old-entry)))
      (is (= v (kvdb/value old-entry)))

      (is (nil? new-entry))
      )))


(defspec transact-values-concurrent-modifications
  (prop/for-all [k gen/string
                 fk gen/keyword
                 counter (gen/large-integer* {:min 1 :max 150})]
    (and-let
      [db (kvdb/to-kvdb (atom {}))
       created (kvdb/create! db k {fk 0})
       tx-entries (doall (pmap (fn [_] (kvdb/transact! db k update fk inc))
                               (range counter)))
       final (kvdb/fetch db k)]
      (is (= {fk counter} (kvdb/value final)))
      (is (= counter (count (distinct (map kvdb/revision tx-entries)))))
      (is (= counter (count (distinct (map (comp fk kvdb/value) tx-entries)))))
      )))


(defspec transact-values-concurrent-create-replace-remove
  (prop/for-all [k gen/string
                 f gen/keyword
                 third (gen/large-integer* {:min 1 :max 50})]
    (and-let
      [db (kvdb/to-kvdb (atom {}))
       tx (fn [v _]
            (cond (nil? v)   {}
                  (empty? v) (assoc v f 0)
                  :else      (kvdb/removal v)))
       tx-entries (doall (pmap (partial kvdb/transact! db k tx)
                               (range (* 3 third))))]

      (is (= {nil third, {} third, {f 0} third}
             (frequencies (map kvdb/value tx-entries))))

      (is (nil? (kvdb/fetch db k)))
      )))


(defspec transact-values-max-attempts
  (prop/for-all [k gen/string
                 attempts (gen/large-integer* {:min 1 :max 60})
                 impl (gen/elements
                        [{`proto/create!  (fn crc [db k v] (throw (kvdb/key-collision db ::kvdb/create! k)))}
                         {`proto/replace! (fn upk [db k r v] (throw (kvdb/key-not-found db ::kvdb/replace! k)))}
                         {`proto/replace! (fn upr [db k r v] (throw (kvdb/revision-mismatch db ::kvdb/replace! k r (inc r))))}
                         {`proto/remove!  (fn rmk [db k r] (throw (kvdb/key-not-found db ::kvdb/remove! k)))}
                         {`proto/remove!  (fn rmr [db k r] (throw (kvdb/revision-mismatch db ::kvdb/remove! k r (inc r))))}
                         ])]
    (and-let
      [failing-op (first (keys impl))
       db (kvdb/override (kvdb/to-kvdb (atom {})) impl)
       counters (atom {`proto/create!  0
                       `proto/replace! 0
                       `proto/remove!  0})
       tx (fn [v]
            (cond (nil? v)   (do (swap! counters update `proto/create! inc)
                                 {})
                  (empty? v) (do (swap! counters update `proto/replace! inc)
                                 (assoc v :x 0))
                  :else      (do (swap! counters update `proto/remove! inc)
                                 (kvdb/removal v))))]
      (is (kvdb/exceeded-transact-attempts-exception?
            (is (thrown? Exception
                  (kvdb/with-max-transact-attempts attempts
                    (do (kvdb/transact-values! db k tx)
                        (kvdb/transact-values! db k tx)
                        (kvdb/transact-values! db k tx)))))))
      (is (= attempts (get @counters failing-op))
          (format "Expected attempts %s on %s but %s"
                  attempts failing-op @counters)))))


(defn throws
  "Returns a function that always throws the provided exception."
  [ex]
  (fn [& _] (throw ex)))


(deftest transact-values-exception-handling 50
  (and-let
    [db (kvdb/to-kvdb (atom {}))]
    (is (thrown-with-msg? Exception #"Boom TX"
          (kvdb/transact-values! db "abc"
            (fn [v] (when (nil? v)
                      (throw (Exception. "Boom TX"))))))
        "Unexpected transaction exceptions are rethrown")

    (is (thrown-with-msg? Exception #"Not Created"
          (kvdb/transact!
            (kvdb/override db `proto/create!
              (throws (Exception. "Not Created")))
            "abc"
            (constantly {:wrong "value"})))
        "Unexpected exceptions when creating an entry are rethrown.")

    [_ (kvdb/transact! db "abc" (constantly {:abc "DEF" :x 1}))]

    (is (thrown-with-msg? Exception #"Not Replaced"
          (kvdb/transact!
            (kvdb/override db `proto/replace!
              (throws (Exception. "Not Replaced")))
            "abc"
            update :x inc))
        "Unexpected exceptions when replacing an entry are rethrown.")

    (is (thrown-with-msg? Exception #"Not Removed"
          (kvdb/transact!
            (kvdb/override db `proto/remove!
              (throws (Exception. "Not Removed")))
            "abc"
            kvdb/removal))
        "Unexpected exceptions when updating an entry are rethrown.")
    ))


(defn ->hashmap-kvdb
  [entries]
  (kvdb/to-kvdb (into {} entries)))

(defspec kvdb-hashmap-spec 50
  (kvdb-properties ->hashmap-kvdb))


(defn ->treemap-kvdb
  [entries]
  (kvdb/to-kvdb (into (sorted-map) entries)))

(defspec kvdb-treemap-spec 50
  (kvdb-properties ->treemap-kvdb))


(defn ednize-vals
  [m]
  (zipmap (keys m)
          (map pr-str (vals m))))


(defn read-edn-vals
  [m]
  (zipmap (keys m)
          (map read-string (vals m))))


(defspec kvdb-coerced-spec 70
  (kvdb-properties
    (gen/fmap (fn [->kvdb]
                #(kvdb/coerced (->kvdb (zipmap (keys %)
                                               (map ednize-vals (vals %))))
                               read-edn-vals
                               ednize-vals))
              (gen/elements [->hashmap-kvdb
                             ->treemap-kvdb]))))

