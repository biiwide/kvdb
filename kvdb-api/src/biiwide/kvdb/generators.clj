(ns biiwide.kvdb.generators
  (:refer-clojure :exclude [key])
  (:require [biiwide.kvdb :as kvdb]
            [clojure.test.check.generators :as gen]))


(defn key []
  gen/string)


(defn value []
  (gen/map gen/simple-type-printable-equatable
           (gen/scale #(int (* 0.3 %)) gen/any-printable-equatable)
           {:max-elements 20}))


(defn entry []
  (gen/let [k (key)
            v (value)
            r gen/nat]
    (kvdb/entry k v r)))


(defn data
  ([]
   (data nil))
  ([opts]
   (gen/map (:key opts (key))
            (:value opts (value))
            (merge {:max-elements 65} opts))))


(defn readable-kvdb [& {:as opts}]
  (gen/fmap kvdb/to-kvdb (data opts)))


(defn pageable-kvdb []
  (gen/fmap #(kvdb/to-kvdb (into (sorted-map) %))
            (data)))


(defn mutable-kvdb []
  (gen/fmap (comp kvdb/to-kvdb atom)
            (pageable-kvdb)))
