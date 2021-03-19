(defproject biiwide.kvdb/kvdb-parent "0.1.0"

  :description "A reusable protocol for KeyValue DataBases."

  :url "https://github.com/biiwide/kvdb"

  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}

  :plugins [[lein-modules "0.3.0"]
            [lein-codox "0.10.3"]]

  :modules {:versions {biiwide.kvdb :version}}

  :codox {:output-path "codox"
          :source-uri "http://github.com/biiwide/kvdb/blob/{version}/{filepath}#L{line}"
          :source-paths ["kvdb-api/src"
                         "kvdb-aws-dynamodb/src"]}

  :aliases {"test"     ["modules" "test"]
            "test-all" ["modules" "test-all"]}

  :release-tasks [["vcs" "assert-committed"]
                  ["change" "version" "leiningen.release/bump-version" "release"]
                  ["modules" "change" "version" "leiningen.release/bump-version" "release"]
                  ["vcs" "commit"]
                  ["vcs" "tag" "--no-sign"]
                  ["modules" "deploy"]
                  ["change" "version" "leiningen.release/bump-version"]
                  ["modules" "change" "version" "leiningen.release/bump-version"]
                  ["vcs" "commit"]
                  #_["vcs" "push"]]
  )
