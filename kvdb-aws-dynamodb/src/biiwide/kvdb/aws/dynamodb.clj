(ns biiwide.kvdb.aws.dynamodb
  (:require [amazonica.aws.dynamodbv2 :as ddb]
            [biiwide.kvdb :as kvdb]
            [biiwide.kvdb.protocols :as proto])
  (:import  [com.amazonaws.services.dynamodbv2.model ConditionalCheckFailedException]))


(defn unwrap-item
  [{:keys [key value revision]}]
  (kvdb/entry key value revision))


(defn scan-all
  [credentials scan-request]
  (lazy-seq
    (let [{:keys [items last-evaluated-key]}
          (ddb/scan credentials scan-request)]
      (if last-evaluated-key
        (concat items
                (scan-all credentials (assoc scan-request :exclusive-start-key last-evaluated-key)))
        items))))

(defn create-wrapped-table-request
  [table-name]
  {:table-name table-name
   :attribute-definitions [{:attribute-name "key" :attribute-type "S"}]
   :key-schema [{:attribute-name "key" :key-type "HASH"}]
   :billing-mode "PAY_PER_REQUEST"})

(deftype WrappedDynamoKVDB
  [credentials table-name]

  proto/ReadableKVDB
  (readable-kvdb? [_] true)
  (fetch [_ k missing-v]
    (let [{:keys [item]}
          (ddb/get-item credentials
                        {:table-name table-name
                         :key {:key {:S k}}})]
      (if (some? item)
        (unwrap-item item)
        missing-v)))

  (entries [_]
    (map unwrap-item (scan-all credentials {:table-name table-name})))

  proto/MutableKVDB
  (mutable-kvdb? [_] true)
  (create! [this k v]
    (try
      (ddb/put-item credentials
                    {:table-name table-name
                     :item {:key k :value v :revision 0}
                     :condition-expression "attribute_not_exists(#K)"
                     :expression-attribute-names {"#K" "key"}})
      (kvdb/entry k v 0)
      (catch ConditionalCheckFailedException e
        (throw (kvdb/key-collision this ::kvdb/create! k)))))

  (remove! [this k rev]
    (try
      (let [{:keys [attributes]}
            (ddb/delete-item credentials
                             {:table-name table-name
                              :key {:key k}
                              :return-values "ALL_OLD"
                              :condition-expression "#R = :rev"
                              :expression-attribute-names {"#R" "revision"}
                              :expression-attribute-values {":rev" rev}})]
        (unwrap-item attributes))
      (catch ConditionalCheckFailedException e
        (throw (kvdb/revision-mismatch this ::kvdb/remove! k -1 rev)))))

  (replace! [this k rev new-v]
    (try
      (ddb/put-item credentials
                    {:table-name table-name
                     :item {:key k :value new-v :revision rev}
                     :condition-expression "#R = :rev"
                     :expression-attribute-names {"#R" "revision"}
                     :expression-attribute-values {":rev" rev}})
      (kvdb/entry k new-v rev)
      (catch ConditionalCheckFailedException e
        (throw (kvdb/revision-mismatch this ::kvdb/replace! k -1 rev)))))

  proto/PageableKVDB
  (pageable-kvdb? [_] true)
  (page [_ exclusive-start-key limit]
    (map unwrap-item
         (scan-all credentials {:table-name table-name
                                :exclusive-start-key exclusive-start-key
                                :limit limit})))

  proto/OverridableKVDB
  (overridable-kvdb? [_] true)
  (push-overrides [this implementations]
    (vary-meta this kvdb/merge-overridden-methods implementations))
  (overridden [this]
    (meta this))
  (pop-overrides [this]
    (vary-meta this kvdb/pop-overridden-methods))
  )
