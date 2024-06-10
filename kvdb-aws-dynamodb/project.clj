(defproject biiwide/kvdb-aws-dynamodb "0.1.3"

  :description "KVDB implementation(s) for AWS DynamoDB."

  :url "https://github.com/biiwide/kvdb"

  :license {:name "EPL-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}

  :repositories {"dynamodb-local-oregon" "https://s3-us-west-2.amazonaws.com/dynamodb-local/release"}

  :plugins [[biiwide/copy-deps "0.7.1"]
            [lein-ancient "1.0.0-RC3"]]

  :dependencies [[amazonica "0.3.166"
                  :exclusions [com.amazonaws/aws-java-sdk
                               com.amazonaws/amazon-kinesis-client
                               com.amazonaws/dynamodb-streams-kinesis-adapter
                               com.taoensso/nippy]]
                 [biiwide/kvdb-api "0.1.2"]
                 [com.amazonaws/aws-java-sdk-dynamodb "1.12.739"]
                 [org.clojure/clojure "1.10.3" :scope "provided"]]

  :repl-options {:init-ns biiwide.kvdb.aws.dynamodb}

  :profiles
  {:dev
   {:dependencies [[com.amazonaws/DynamoDBLocal "1.20.0"]
                   [org.clojure/test.check "1.1.1"]]

    :prep-tasks   ["javac" "compile" "copy-deps"]

    :copy-deps    {:dependencies [[com.almworks.sqlite4java/libsqlite4java-linux-amd64 "1.0.392" :extension "so"]
                                  [com.almworks.sqlite4java/libsqlite4java-linux-i386 "1.0.392" :extension "so"]
                                  [com.almworks.sqlite4java/libsqlite4java-osx "1.0.392" :extension "dylib"]
                                  [io.github.ganadist.sqlite4java/libsqlite4java-osx-aarch64 "1.0.392" :extension "dylib"]
                                  [com.almworks.sqlite4java/sqlite4java-win32-x64 "1.0.392" :extension "dll"]
                                  [com.almworks.sqlite4java/sqlite4java-win32-x86 "1.0.392" :extension "dll"]]
                   :destination  "native-libs"}

    :jvm-opts     ["-Djava.library.path=native-libs/com.almworks.sqlite4java:native-libs/io.github.ganadist.sqlite4java"]
    }
   }
  )
