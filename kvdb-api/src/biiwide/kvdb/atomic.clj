(ns biiwide.kvdb.atomic
  "Implements the KVDB Protocols for Atoms."
  (:require [biiwide.kvdb :as kvdb]
            [biiwide.kvdb.protocols :as proto]))


(deftype Wrapper
  [a _meta]
  proto/KVDBable
  (to-kvdb [aw] aw)

  clojure.lang.IAtom
  (swap [_ f] (.swap a f))
  (swap [_ f x] (.swap a f x))
  (swap [_ f x y] (.swap a f x y))
  (swap [_ f x y args] (.swap a f x y args))
  (compareAndSet [_ old-v new-v] (.compareAndSet old-v new-v))
  (reset [_ new-v] (.reset a new-v))

  clojure.lang.IAtom2
  (swapVals [_ f] (.swapVals a f))
  (swapVals [_ f x] (.swapVals a f x))
  (swapVals [_ f x y] (.swapVals a f x y))
  (swapVals [_ f x y args] (.swapVals a f x y args))
  (resetVals [_ new-v] (.resetVals new-v))

  clojure.lang.IDeref
  (deref [_] (.deref a))

  clojure.lang.IRef
  (setValidator [_ vf] (.setValidator a vf))
  (getValidator [_] (.getValidator a))
  (getWatches [_] (.getWatches a))
  (addWatch [_ k callback] (.addWatch a k callback))
  (removeWatch [_ k] (.removeWatch a k))

  clojure.lang.IMeta
  (meta [aw] _meta)

  clojure.lang.IObj
  (withMeta [aw m] (Wrapper. a m)))

(ns-unmap *ns* '->Wrapper)


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


(extend-type Wrapper
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
        (throw (RuntimeException. "Method not supported"))))))


(extend-type clojure.lang.IAtom2
  proto/KVDBable
  (to-kvdb [a] (Wrapper.
                 (atom (kvdb/to-kvdb @a))
                 (meta a))))


(defmethod print-method Wrapper
  [wrapper writer]
  (.write writer "#biiwide.kvdb/atom[")
  (print-method @wrapper writer)
  (.write writer " 0x")
  (.write writer (Integer/toHexString
                   (System/identityHashCode wrapper)))
  (.write writer "]"))
