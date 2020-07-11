(ns biiwide.kvdb.atomic-test
  (:require [biiwide.kvdb :as kvdb]
            [biiwide.kvdb.atomic :as a]
            [biiwide.kvdb.generators :as kgen]
            [biiwide.kvdb.proto.verification
             :refer [kvdb-properties without-instrumentation]]
            [#?(:clj clojure.test :cljs cljs.test)
             :refer [deftest is are]]
            [clojure.test.check.clojure-test
             :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop])
  #?(:clj (:import (clojure.lang ExceptionInfo))))


(deftest test-invalid-source-value
  (are [m ex-message]
    (kvdb/illegal-argument-exception?
      (is (thrown-with-msg? ExceptionInfo ex-message
            (kvdb/to-kvdb m))))

    (atom nil)            #"(?i)\bCannot create a KVDB\b"
    (atom true)           #"(?i)\bCannot create a KVDB\b"
    (atom 999)            #"(?i)\bCannot create a KVDB\b"
    (atom "z")            #"(?i)\bCannot create a KVDB\b"
    (atom [])             #"(?i)\bCannot create a KVDB\b"

    (atom {nil {}})       #"(?i)\bInvalid key\b"
    (atom {true {}})      #"(?i)\bInvalid key\b"
    (atom {123 {}})       #"(?i)\bInvalid key\b"
    (atom {:two {}})      #"(?i)\bInvalid key\b"

    (atom {"111" nil})    #"(?i)\bInvalid value\b"
    (atom {"222" true})   #"(?i)\bInvalid value\b"
    (atom {"333" 444})    #"(?i)\bInvalid value\b"
    (atom {"444" "abc"})  #"(?i)\bInvalid value\b"
    ))


(deftest test-pageable-support
  (without-instrumentation
    (is (false? (kvdb/pageable-kvdb?
                  (kvdb/to-kvdb (atom {})))))

    (is (true? (kvdb/pageable-kvdb?
                 (kvdb/to-kvdb (atom (sorted-map))))))

    (is (thrown-with-msg? ExceptionInfo #"(?i)\bnot supported\b"
          (kvdb/page (kvdb/to-kvdb (atom {})))))))


(defspec kvdb-atom-spec 50
  (prop/for-all [result (gen/bind (kgen/atomic-kvdb)
                                  #(kvdb-properties % kgen/value))]
    result))
