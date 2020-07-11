(ns biiwide.kvdb.generators
  (:require [biiwide.kvdb :as kvdb]
            [biiwide.kvdb.atomic :as atomic]
            [clojure.test.check.generators :as gen]))


(def value
  (gen/map gen/simple-type-printable-equatable
           gen/any-printable-equatable))


(defn hashmap-kvdb
  ([]
   (hashmap-kvdb gen/string value))
  ([key-generator value-generator]
   (gen/fmap kvdb/to-kvdb
             (gen/map key-generator value-generator))))


(def sortedmap-kvdb
  (comp (partial gen/fmap (comp kvdb/to-kvdb
                                (partial into (sorted-map))))
        hashmap-kvdb))


(defn atomic-kvdb
  ([]
   (atomic-kvdb
     (gen/one-of [(hashmap-kvdb)
                  (sortedmap-kvdb)])))
  ([map-kvdb-generator]
   (gen/fmap (comp kvdb/to-kvdb atom)
             map-kvdb-generator)))
