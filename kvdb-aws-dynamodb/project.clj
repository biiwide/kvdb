(defproject biiwide.kvdb/kvdb-aws-dynamodb "0.1.0"

  :description "KVDB implementation(s) for AWS DynamoDB."

  :url "https://github.com/biiwide/kvdb"

  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}

  :repositories {"dynamodb-local-oregon" "https://s3-us-west-2.amazonaws.com/dynamodb-local/release"}

  :dependencies [[amazonica "0.3.153"
                  :exclusions [com.amazonaws/aws-java-sdk
                               com.amazonaws/amazon-kinesis-client
                               com.amazonaws/dynamodb-streams-kinesis-adapter]]
                 [biiwide.kvdb/kvdb-api "0.1.0-SNAPSHOT"]
                 [org.clojure/clojure "1.10.1"]]

  :repl-options {:init-ns biiwide.kvdb.aws.dynamodb}

  :plugins [[biiwide/copy-deps "0.6.0"]]

  :profiles
  {:dev
   {:dependencies [[com.amazonaws/DynamoDBLocal "1.13.6"]
                   [org.clojure/test.check "1.1.0"]]

    :prep-tasks   ["javac" "compile" "copy-deps"]

    :copy-deps    {:dependencies [[com.almworks.sqlite4java/libsqlite4java-linux-amd64 "1.0.392" :extension "so"]
                                  [com.almworks.sqlite4java/libsqlite4java-linux-i386 "1.0.392" :extension "so"]
                                  [com.almworks.sqlite4java/libsqlite4java-osx "1.0.392" :extension "dylib"]
                                  [com.almworks.sqlite4java/sqlite4java-win32-x64 "1.0.392" :extension "dll"]
                                  [com.almworks.sqlite4java/sqlite4java-win32-x86 "1.0.392" :extension "dll"]]
                   :destination  "native-libs"}

    :jvm-opts     ["-Djava.library.path=native-libs/com.almworks.sqlite4java"]
    }
   }

  )
