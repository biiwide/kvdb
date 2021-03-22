(ns biiwide.kvdb.aws.dynamodb-test
  (:require [amazonica.core :as aws]
            [amazonica.aws.dynamodbv2 :as ddb]
            [biiwide.kvdb :as kvdb]
            [biiwide.kvdb.aws.dynamodb :as dyn-kvdb]
            [biiwide.kvdb.verification :as verify]
            [biiwide.kvdb.generators :as kvdb-gen]
            [clojure.test :refer [deftest is are]]
            [clojure.test.check :as tc]
            [clojure.test.check.clojure-test :as ct
             :refer [defspec]]
            [clojure.test.check.generators :as gen])
  (:import (com.amazonaws.services.dynamodbv2.local.main ServerRunner)
           (com.amazonaws.services.dynamodbv2.local.server DynamoDBProxyServer)
           (java.io ByteArrayOutputStream PrintStream)
           (java.net ServerSocket)))


(defn valid-listener-port?
  "Determine if a value is possibly valid port number for a service."
  [x]
  (and (integer? x)
       (<= 1024 x 65535)))


(defn find-available-port
  "Find the next available local service port number."
  []
  (with-open [sock (ServerSocket. 0)]
    (.getLocalPort sock)))


(defn ^DynamoDBProxyServer start-server
  ([]
   (start-server {}))
  ([{:keys [port in-memory? shared-db?]
     :or   {in-memory? true
            shared-db? true}}]
   (let [args (concat (when in-memory? ["-inMemory"])
                      (when shared-db? ["-sharedDb"])
                      ["-port" (str (if (valid-listener-port? port)
                                      port
                                      (find-available-port)))])]
     (let [out System/out]
       (try
         (System/setOut (PrintStream. (ByteArrayOutputStream.)))
         (doto (ServerRunner/createServerFromCommandLineArgs
                 (into-array String args))
               (.start))
         (finally
           (System/setOut out)))))))


(defn aws-client-config []
  (merge @@#'aws/client-config
         @#'aws/*client-config*))


(defn dynamo-client-builder
  [endpoint aws-client-fn]
  (fn [clazz credential client-config]
    (if (= com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient clazz)
      (-> (#'aws/builder com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient)
          (.disableEndpointDiscovery)
          (.withCredentials (aws/get-credentials {:access-key "" :secret-key ""}))
          (.withClientConfiguration (#'aws/get-client-configuration {}))
          (.withEndpointConfiguration
            (com.amazonaws.client.builder.AwsClientBuilder$EndpointConfiguration.
              endpoint "us-east-1"))
          (.build))
      (aws-client-fn clazz credential client-config))))


(defn with-dynamodb*
  [f table-definitions]
  (let [port     (find-available-port)
        server   (start-server {:port port})
        endpoint (format "http://localhost:%s" port)
        creds    (assoc @#'aws/*credentials* :endpoint endpoint)
        client-cfg (aws-client-config)]
    (try (aws/with-client-config
           (assoc client-cfg :amazon-client-fn
                  (dynamo-client-builder endpoint (:amazon-client-fn client-cfg)))
           (aws/with-credential creds
             (doseq [table-def table-definitions]
               (ddb/create-table creds table-def))
             (f creds)))
      (finally (.stop server)))))


(defmacro with-dynamodb
  [[creds-binding & [table-definitions]] & body]
  `(with-dynamodb*
     (fn [~creds-binding] ~@body)
     ~table-definitions))


(deftest check-protocol-implementations
  (with-dynamodb [creds [(dyn-kvdb/create-wrapped-table-request "proto-check")]]
    (let [db (dyn-kvdb/wrapped-table "proto-check")]
      (is (kvdb/readable-kvdb? db))
      (is (kvdb/pageable-kvdb? db))
      (is (kvdb/mutable-kvdb? db))
      (is (kvdb/overridable-kvdb? db)))))


(defn choose-char
  [min-char max-char]
  (gen/fmap char (gen/choose min-char max-char)))

(def gen-table-name-char
  (gen/frequency [[26 (choose-char \a \z)]
                  [26 (choose-char \A \Z)]
                  [10 (choose-char \0 \9)]
                  [1  (gen/return \_)]
                  [1  (gen/return \-)]
                  [1  (gen/return \.)]]))
                    
(def gen-table-name
  (gen/fmap (partial apply str)
            (gen/vector gen-table-name-char 3 255)))

(def gen-dynamodb-key
  (gen/such-that not-empty gen/string))

(defn coerce-edn
  [edn-map]
  (read-string
    (get edn-map :edn)))

(defn uncoerce-edn
  [x]
  {:edn (pr-str x)})


(defn wrapped-table-context
  [creds]
  (gen/let [table-name gen-table-name
            data       (kvdb-gen/data {:key gen-dynamodb-key
                                       :max-elements 20})
            page-size  (gen/choose 2 21)]
    (fn [property]
      (ddb/create-table creds (dyn-kvdb/create-wrapped-table-request table-name))
      (try
        (let [creds    (assoc creds :access-key "a" :secret-key "a" :region "us-east-1")
              dyn-kvdb (-> (dyn-kvdb/wrapped-table creds table-name
                                                   {:page-size page-size})
                           (kvdb/coerced {:coerce   coerce-edn
                                          :uncoerce uncoerce-edn}))]
          (doseq [[k v] data]
            (kvdb/create! dyn-kvdb k v))
          (property {:kvdb dyn-kvdb}))
        (finally
          (ddb/delete-table creds {:table-name table-name}))))))


(defmacro def-dynamodb-spec
  {:arglists '([name property] [name num-tests? property] [name options? property])}
  ([name ddb-binding property]
   `(def-dynamodb-spec ~name nil ~ddb-binding ~property))
  ([name options ddb-binding property]
   `(defn ~(vary-meta name assoc
                      ::defspec true
                      :test `(fn []
                               (ct/assert-check
                                 (assoc (~name) :test-var (str '~name)))))
      {:arglists '([] ~'[num-tests & {:keys [seed max-size reporter-fn]}])}
      ([] (let [options# (ct/process-options ~options)]
            (apply ~name (:num-tests options#) (apply concat options#))))
      ([times# & {:as quick-check-opts#}]
       (let [options# (merge (ct/process-options ~options) quick-check-opts#)]
         (with-dynamodb ~ddb-binding
           (apply
            tc/quick-check
            times#
            (vary-meta ~property assoc :name '~name)
            (apply concat options#))))))))

(def-dynamodb-spec wrapped-table-readable-properties
  [creds]
  (verify/within-context (wrapped-table-context creds)
                          verify/readable-kvdb-properties))


(def-dynamodb-spec wrapped-table-pageable-properties
  [creds]
  (verify/within-context (wrapped-table-context creds)
    verify/pageable-kvdb-properties))

(def-dynamodb-spec wrapped-table-overridable-properties
  [creds]
  (verify/within-context (wrapped-table-context creds)
    verify/overridable-kvdb-properties))

(def-dynamodb-spec wrapped-table-mutable-properties
  [creds]
  (verify/within-context (wrapped-table-context creds)
    (verify/mutable-kvdb-properties gen-dynamodb-key (kvdb-gen/value))))

(def-dynamodb-spec wrapped-table-transact-properties 50
  [creds]
  (verify/within-context (wrapped-table-context creds)
    (verify/transact-kvdb-properties gen-dynamodb-key (kvdb-gen/value))))
