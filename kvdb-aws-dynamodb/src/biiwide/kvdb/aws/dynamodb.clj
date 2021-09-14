(ns biiwide.kvdb.aws.dynamodb
  (:require [amazonica.aws.dynamodbv2 :as ddb]
            [biiwide.kvdb :as kvdb]
            [biiwide.kvdb.protocols :as proto]
            [clojure.spec.alpha :as s])
  (:import  [com.amazonaws.services.dynamodbv2.model ConditionalCheckFailedException]))


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


(defrecord GenericDynamoKVDB
  [credentials
   ^String table-name
   key-field
   revision-field
   item-to-entry
   entry-to-item
   ^int page-size])

(extend-type GenericDynamoKVDB
  proto/ReadableKVDB
  (readable-kvdb? [^GenericDynamoKVDB db] true)
  (fetch [^GenericDynamoKVDB db k missing-v]
    (let [{:keys [item]}
          (ddb/get-item (.credentials db)
                        {:table-name (.table-name db)
                         :key {(.key-field db) k}
                         :consistent-read true})]
      (if (some? item)
        ((.item-to-entry db) item)
        missing-v)))

  (entries [^GenericDynamoKVDB db]
    (map (.item-to-entry db)
         (scan-all (.credentials db)
                   {:table-name (.table-name db)})))

  proto/MutableKVDB
  (mutable-kvdb? [^GenericDynamoKVDB db] true)
  (create! [^GenericDynamoKVDB db k v]
    (try
      (let [new-entry (kvdb/entry k v 0)]
        (ddb/put-item (.credentials db)
                      {:table-name (.table-name db)
                       :item ((.entry-to-item db) new-entry)
                       :condition-expression "attribute_not_exists(#K)"
                       :expression-attribute-names {"#K" (.key-field db)}})
        new-entry)
      (catch ConditionalCheckFailedException e
        (throw (kvdb/key-collision db ::kvdb/create! k)))))

  (remove! [^GenericDynamoKVDB db k rev]
    (try
      (let [{:keys [attributes]}
            (ddb/delete-item (.credentials db)
                             {:table-name (.table-name db)
                              :key {(.key-field db) k}
                              :return-values "ALL_OLD"
                              :condition-expression "#R = :rev"
                              :expression-attribute-names {"#R" (.revision-field db)}
                              :expression-attribute-values {":rev" rev}})]
        ((.item-to-entry db) attributes))
      (catch ConditionalCheckFailedException e
        (throw (kvdb/revision-mismatch db ::kvdb/remove! k 0 rev)))))

  (replace! [^GenericDynamoKVDB db k rev new-v]
    (try
      (let [new-entry (kvdb/entry k new-v (inc rev))]
        (ddb/put-item (.credentials db)
                      {:table-name (.table-name db)
                       :item ((.entry-to-item db) new-entry)
                       :condition-expression "#R = :rev"
                       :expression-attribute-names {"#R" (.revision-field db)}
                       :expression-attribute-values {":rev" rev}})
        new-entry)
      (catch ConditionalCheckFailedException e
        (throw (kvdb/revision-mismatch db ::kvdb/replace! k 0 rev)))))

  proto/PageableKVDB
  (pageable-kvdb? [^GenericDynamoKVDB db] true)
  (page [^GenericDynamoKVDB db exclusive-start-key limit]
    (->> (scan-all (.credentials db)
                   (cond-> {:table-name (.table-name db)
                            :limit      (scan-limit limit (.page-size db))}
                     (not (empty? exclusive-start-key))
                     (assoc :exclusive-start-key {(.key-field db) exclusive-start-key})))
         (map (.item-to-entry db))
         (take (or limit (.page-size db)))))
  )


(s/def ::dynamodb-ident
  (s/or :string string?
        :keyword simple-keyword?))

(s/def ::credentials (s/nilable map?))
(s/def ::table-name  ::dynamodb-ident)
(s/def ::key-field ::dynamodb-ident)
(s/def ::revision-field ::dynamodb-ident)
(s/def ::item-to-entry fn?)
(s/def ::entry-to-item fn?)
(s/def ::page-size nat-int?)

(s/def ::generic-table-options
  (s/keys :req-un [::credentials ::table-name ::key-field ::revision-field
                   ::item-to-entry ::entry-to-item
                   ::page-size]))

(s/fdef generic-table
  :args (s/cat :options ::generic-table-options)
  :ret  kvdb/kvdb?)

(defn generic-table
  [{:keys [credentials table-name
           key-field revision-field
           item-to-entry entry-to-item
           page-size]}]
  {:pre [(some? table-name)
         (some? key-field)
         (some? revision-field)
         (not= key-field revision-field)
         (fn? item-to-entry)
         (fn? entry-to-item)
         (nat-int? page-size)]}
  (GenericDynamoKVDB.
    credentials table-name key-field revision-field
    item-to-entry entry-to-item page-size))

(ns-unmap *ns* '->GenericDynamoKVDB)
(ns-unmap *ns* 'map->GenericDynamoKVDB)


(defn create-wrapped-table-request
  "Constructs a request data for creating a DynamoDB table
  suitable for use with a wrapped-table KVDB instance."
  [table-name]
  {:table-name            table-name
   :attribute-definitions [{:attribute-name "key" :attribute-type "S"}]
   :key-schema            [{:attribute-name "key" :key-type "HASH"}]
   :billing-mode          "PAY_PER_REQUEST"})


(defn ^:private unwrap-item
  [{:keys [key value revision]}]
  (kvdb/entry key value revision))


(defn ^:private wrap-item
  [entry]
  {:key      (kvdb/key entry)
   :value    (kvdb/value entry)
   :revision (kvdb/revision entry)})


(defn wrapped-table
  "Build a KVDB instance supporting a DynamoDB table with :key,
  :value, & :revision fields.
  The HASH key field of the table is expected to be :key."
  ([table-name]
   (wrapped-table {} table-name {}))
  ([aws-creds table-name
    & [{:keys [page-size]
        :or   {page-size 500}}]]
   (generic-table
     {:credentials    aws-creds
      :table-name     table-name
      :key-field      :key
      :revision-field :revision
      :item-to-entry  unwrap-item
      :entry-to-item  wrap-item
      :page-size      page-size})))
