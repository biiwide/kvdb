(ns biiwide.kvdb.atomic
  "Implements the KVDB Protocols for Atoms."
  (:require [biiwide.kvdb :as kvdb]
            [biiwide.kvdb.protocols :as proto]))


(defn ^:private do-create!
  [m k v]
  (if (nil? (proto/fetch m k nil))
    (assoc m k (kvdb/entry k v 0))
    (throw (kvdb/key-collision m ::kvdb/create! k))))


(defn ^:private do-remove!
  [m k rev]
  (let [ent (proto/fetch m k nil)]
    (cond (nil? ent)
          (throw (kvdb/key-not-found m ::kvdb/remove! k))
          (not= rev (kvdb/revision ent))
          (throw (kvdb/revision-mismatch m ::kvdb/remove!
                                         k (kvdb/revision ent) rev))
          :else (dissoc m k))))


(defn ^:private do-replace!
  [m k rev new-v]
  (let [ent (proto/fetch m k nil)]
    (cond (nil? ent)
          (throw (kvdb/key-not-found m ::kvdb/replace! k))
          (not= rev (kvdb/revision ent))
          (throw (kvdb/revision-mismatch m ::kvdb/replace!
                                         k (kvdb/revision ent) rev))
          :else (assoc m k (kvdb/entry k new-v (inc rev))))))


(extend-type clojure.lang.IAtom2
  proto/KVDBable
  (to-kvdb [a]
    (atom (kvdb/to-kvdb @a)))

  proto/ReadableKVDB
  (readable-kvdb? [a]
    (proto/readable-kvdb? @a))
  (fetch [a k missing-v]
    (proto/fetch @a k missing-v))
  (entries [a]
    (kvdb/entries @a))

  proto/MutableKVDB
  (mutable-kvdb? [a]
    (proto/readable-kvdb? @a))
  (create! [a k v]
    (proto/fetch (swap! a do-create! k v)
                 k nil))
  (remove! [a k rev]
    (proto/fetch (first (swap-vals! a do-remove! k rev))
                 k nil))
  (replace! [a k rev new-v]
    (proto/fetch (swap! a do-replace! k rev new-v)
                 k nil))

  proto/PageableKVDB
  (pageable-kvdb? [a]
    (proto/pageable-kvdb? @a))
  (page [a exclusive-start-key limit]
    (let [v @a]
      (if (proto/pageable-kvdb? v)
        (proto/page v exclusive-start-key limit)
        (throw (RuntimeException. "Method not supported")))))

  proto/OverridableKVDB
  (overridable-kvdb? [a]
    (proto/readable-kvdb? @a))
  (push-overrides [a implementations]
    (atom @a :meta (kvdb/merge-overridden-methods (meta a) implementations)
             :validator (get-validator a)))
  (overridden [a]
    (meta a))
  (pop-overrides [a]
    (atom @a :meta (kvdb/pop-overridden-methods (meta a))
             :validator (get-validator a)))
  )
