(ns biiwide.kvdb.atomic-test
  (:require [biiwide.kvdb :as kvdb]
            [biiwide.kvdb.atomic :as a]
            [biiwide.kvdb.generators :as kgen]
            [biiwide.kvdb.proto.verification
             :refer [kvdb-properties without-instrumentation]]
            [clojure.test :refer :all]
            [clojure.test.check.clojure-test
             :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]))


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


(defspec kvdb-atom-spec 50
  (prop/for-all [result (gen/bind (kgen/atomic-kvdb)
                                  #(kvdb-properties % kgen/value))]
    result))
