(ns biiwide.kvdb.aws.dynamodb-test
  (:require [amazonica.core :as aws]
            [amazonica.aws.dynamodbv2 :as ddb]
            [biiwide.kvdb :as kvdb]
            [biiwide.kvdb.aws.dynamodb :as dynkvdb]
            [biiwide.kvdb.proto.verification :as verify]
            [clojure.test :refer [deftest is are]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen])
  (:import (com.amazonaws.services.dynamodbv2.local.main ServerRunner)
           (com.amazonaws.services.dynamodbv2.local.server DynamoDBProxyServer)
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
     (doto (ServerRunner/createServerFromCommandLineArgs
             (into-array String args))
           (.start)))))


(defmacro with-dynamodb
  [[creds-binding & [table-definitions]] & body]
  `(let [port# (find-available-port)
         server# (start-server {:port port#})
         ~creds-binding {:endpoint (format "http://localhost:%s" port#)}]
     (try (do (doseq [table-def# ~table-definitions]
                (ddb/create-table ~creds-binding table-def#))
              (aws/with-credential (merge @#'aws/*credentials*
                                          ~creds-binding)
                ~@body))
       (finally (.stop server#)))))


(deftest test-pageable-support
  (verify/without-instrumentation
    (is (false? (kvdb/pageable-kvdb?
                  (kvdb/to-kvdb (atom {})))))

    (is (true? (kvdb/pageable-kvdb?
                 (kvdb/to-kvdb (atom (sorted-map))))))

    (is (thrown-with-msg? Exception #"(?i)\bnot supported\b"
          (kvdb/page (kvdb/to-kvdb (atom {})))))))


(defspec kvdb-atom-spec 50
  (verify/kvdb-properties
    (gen/fmap (fn [mapify]
                #(kvdb/to-kvdb (atom (mapify %))))
                  (gen/elements [(partial into {})
                                 (partial into (sorted-map))]))))


(defspec kvdb-atom-pageable-spec 50
  (verify/kvdb-properties #(kvdb/to-kvdb (atom (into (sorted-map) %)))))

(defn my-kvdb-context [generate-entry]
  (gen/let [table-name gen/string
            entries    (gen/list generate-entry)]
    (fn [property]
      (with-dynamo-db [_ (create-wrapped-table table-name)]
        (property {:kvdb (dynamo-kvdb/wrapped-kvdb table-name)})))))



