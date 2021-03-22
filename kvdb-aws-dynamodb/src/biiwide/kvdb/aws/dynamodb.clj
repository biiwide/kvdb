(ns biiwide.kvdb.aws.dynamodb
  (:require [amazonica.aws.dynamodbv2 :as ddb]
            [biiwide.kvdb :as kvdb]
            [biiwide.kvdb.protocols :as proto])
  (:import  [com.amazonaws.services.dynamodbv2.model ConditionalCheckFailedException]))


(defn ^:private unwrap-item
  [{:keys [key value revision]}]
  (kvdb/entry key value revision))


(defn ^:private scan-all
  [credentials scan-request]
  (lazy-seq
    (let [{:keys [items last-evaluated-key]}
          (ddb/scan credentials scan-request)]
      (if last-evaluated-key
        (lazy-cat items
                  (scan-all credentials
                            (assoc scan-request
                                   :exclusive-start-key last-evaluated-key)))
        items))))

(defn ^:private scan-limit
  [n default]
  (-> (or n default)
      (max 1)
      (min 1000)))


(defn create-wrapped-table-request
  [table-name]
  {:table-name table-name
   :attribute-definitions [{:attribute-name "key" :attribute-type "S"}]
   :key-schema [{:attribute-name "key" :key-type "HASH"}]
   :billing-mode "PAY_PER_REQUEST"})


(defrecord WrappedDynamoKVDB
  [credentials
   ^String table-name
   ^int page-size])

(extend-type WrappedDynamoKVDB
  proto/ReadableKVDB
  (readable-kvdb? [^WrappedDynamoKVDB db] true)
  (fetch [^WrappedDynamoKVDB db k missing-v]
    (let [{:keys [item]}
          (ddb/get-item (.credentials db)
                        {:table-name (.table-name db)
                         :key {:key k}})]
      (if (some? item)
        (unwrap-item item)
        missing-v)))

  (entries [^WrappedDynamoKVDB db]
    (map unwrap-item (scan-all (.credentials db)
                               {:table-name (.table-name db)})))

  proto/MutableKVDB
  (mutable-kvdb? [^WrappedDynamoKVDB db] true)
  (create! [^WrappedDynamoKVDB db k v]
    (try
      #_(println "Creating Item:" (pr-str {:key k :value v :revision 0}))
      (ddb/put-item (.credentials db)
                    {:table-name (.table-name db)
                     :item {:key k :value v :revision 0}
                     :condition-expression "attribute_not_exists(#K)"
                     :expression-attribute-names {"#K" "key"}})
      (kvdb/entry k v 0)
      (catch ConditionalCheckFailedException e
        (throw (kvdb/key-collision db ::kvdb/create! k)))))

  (remove! [^WrappedDynamoKVDB db k rev]
    (try
      (let [{:keys [attributes]}
            (ddb/delete-item (.credentials db)
                             {:table-name (.table-name db)
                              :key {:key k}
                              :return-values "ALL_OLD"
                              :condition-expression "#R = :rev"
                              :expression-attribute-names {"#R" "revision"}
                              :expression-attribute-values {":rev" rev}})]
        (unwrap-item attributes))
      (catch ConditionalCheckFailedException e
        (throw (kvdb/revision-mismatch db ::kvdb/remove! k 0 rev)))))

  (replace! [^WrappedDynamoKVDB db k rev new-v]
    (try
      (ddb/put-item (.credentials db)
                    {:table-name (.table-name db)
                     :item {:key k :value new-v :revision (inc rev)}
                     :condition-expression "#R = :rev"
                     :expression-attribute-names {"#R" "revision"}
                     :expression-attribute-values {":rev" rev}})
      (kvdb/entry k new-v (inc rev))
      (catch ConditionalCheckFailedException e
        (throw (kvdb/revision-mismatch db ::kvdb/replace! k 0 rev)))))

  proto/PageableKVDB
  (pageable-kvdb? [^WrappedDynamoKVDB db] true)
  (page [^WrappedDynamoKVDB db exclusive-start-key limit]
    (->> (scan-all (.credentials db)
                   (cond-> {:table-name (.table-name db)
                            :limit      (scan-limit limit (.page-size db))}
                     (not (empty? exclusive-start-key))
                     (assoc :exclusive-start-key {:key exclusive-start-key})))
         (map unwrap-item)
         (take (or limit (.page-size db)))))
  )


(defn wrapped-table
  ([table-name]
   (wrapped-table {} table-name {}))
  ([aws-creds table-name
    & [{:keys [page-size]
        :or   {page-size 500}}]]
   (WrappedDynamoKVDB.
     aws-creds
     table-name
     page-size)))
