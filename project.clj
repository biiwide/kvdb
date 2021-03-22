(defproject biiwide/kvdb-parent "0.1.1-SNAPSHOT"

  :description "A reusable protocol for KeyValue DataBases."

  :url "https://github.com/biiwide/kvdb"

  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}

  :packaging "pom"

  :plugins [[lein-codox "0.10.3"]
            [lein-sub "0.3.0"]]

  :codox {:output-path "codox"
          :source-uri "http://github.com/biiwide/kvdb/blob/{version}/{filepath}#L{line}"
          :source-paths ["kvdb-api/src"
                         "kvdb-aws-dynamodb/src"]}

  :sub ["kvdb-api"
        "kvdb-aws-dynamodb"]

  :aliases {"test"     ["sub" "test"]
            "test-all" ["sub" "test-all"]}

  :release-tasks [["vcs" "assert-committed"]
                  ["change" "version" "leiningen.release/bump-version" "release"]
                  ["modules" "change" "version" "leiningen.release/bump-version" "release"]
                  #_["vcs" "commit"]
                  #_["vcs" "tag" "--no-sign"]
                  #_["modules" "deploy" "clojars"]
                  ["change" "version" "leiningen.release/bump-version"]
                  #_["modules" "change" "version" "leiningen.release/bump-version"]
                  #_["vcs" "commit"]
                  #_["vcs" "push"]]
  )
