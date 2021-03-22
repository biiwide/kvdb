(ns biiwide.kvdb.atomic-test
  (:require [biiwide.kvdb :as kvdb
             :refer [to-kvdb]]
            [biiwide.kvdb.atomic]
            [biiwide.kvdb.generators :as kvdb-gen]
            [biiwide.kvdb.verification :as v
             :refer [without-instrumentation]]
            [clojure.test :refer [are deftest is]]
            [clojure.test.check.clojure-test
             :refer [defspec]]
            [clojure.test.check.generators :as gen]))


(deftest test-readable-kvdb?
  (are [p? v]
    (p? (kvdb/readable-kvdb? v))

    false? (atom nil)
    false? (atom {})
    false? (atom (sorted-map))
    true?  (to-kvdb (atom {}))
    true?  (to-kvdb (atom (sorted-map)))
    ))


(deftest test-pageable-kvdb?
  (are [p? v]
    (p? (kvdb/pageable-kvdb? v))

    false? (atom {})
    false? (atom (sorted-map))
    false? (to-kvdb (atom {}))
    true?  (to-kvdb (atom (sorted-map)))
    ))


(deftest test-mutable-kvdb?
  (are [p? v]
    (p? (kvdb/mutable-kvdb? v))

    false? (atom {})
    false? (atom (sorted-map))
    true?  (to-kvdb (atom {}))
    true?  (to-kvdb (atom (sorted-map)))
    ))


(deftest test-overridable-kvdb?
  (are [p? v]
    (p? (kvdb/overridable-kvdb? v))

    false? (atom {})
    false? (atom (sorted-map))
    true?  (to-kvdb (atom {}))
    true?  (to-kvdb (atom (sorted-map)))
    ))


(deftest test-invalid-source-value
  (are [m ex-class ex-message]
    (is (thrown-with-msg? ex-class ex-message
          (kvdb/to-kvdb m)))

    (atom nil)            RuntimeException          #"(?i)\bCannot create a KVDB\b"
    (atom true)           RuntimeException          #"(?i)\bCannot create a KVDB\b"
    (atom 999)            RuntimeException          #"(?i)\bCannot create a KVDB\b"
    (atom "z")            RuntimeException          #"(?i)\bCannot create a KVDB\b"
    (atom [])             RuntimeException          #"(?i)\bCannot create a KVDB\b"

    (atom {nil {}})       IllegalArgumentException  #"(?i)\bInvalid key\b"
    (atom {true {}})      IllegalArgumentException  #"(?i)\bInvalid key\b"
    (atom {123 {}})       IllegalArgumentException  #"(?i)\bInvalid key\b"
    (atom {:two {}})      IllegalArgumentException  #"(?i)\bInvalid key\b"

    (atom {"111" nil})    IllegalArgumentException  #"(?i)\bInvalid value\b"
    (atom {"222" true})   IllegalArgumentException  #"(?i)\bInvalid value\b"
    (atom {"333" 444})    IllegalArgumentException  #"(?i)\bInvalid value\b"
    (atom {"444" "abc"})  IllegalArgumentException  #"(?i)\bInvalid value\b"
    ))


(deftest test-pageable-support
  (without-instrumentation
    (is (false? (kvdb/pageable-kvdb?
                  (kvdb/to-kvdb (atom {})))))

    (is (true? (kvdb/pageable-kvdb?
                 (kvdb/to-kvdb (atom (sorted-map))))))

    (is (thrown-with-msg? Exception #"(?i)\bnot supported\b"
          (kvdb/page (kvdb/to-kvdb (atom {})))))))


;; AtomicMap
(defn atomic-map-context
  [base-collections]
  (gen/let [base (gen/elements base-collections)
            data (kvdb-gen/data)]
    (fn [prop]
      (prop {:kvdb (kvdb/to-kvdb (atom (into base data)))}))))

(defspec atomic-map-readable-properties
  (v/within-context (atomic-map-context [{} (sorted-map)])
                    v/readable-kvdb-properties))

(defspec atomic-map-pageable-properties
  (v/within-context (atomic-map-context [(sorted-map)])
                    v/pageable-kvdb-properties))

(defspec atomic-map-overridable-properties
  (v/within-context (atomic-map-context [{} (sorted-map)])
                    v/overridable-kvdb-properties))

(defspec atomic-map-mutable-properties
  (v/within-context (atomic-map-context [{} (sorted-map)])
                    (v/mutable-kvdb-properties)))

(defspec atomic-map-transact-properties
  (v/within-context (atomic-map-context [{} (sorted-map)])
                    (v/transact-kvdb-properties)))
