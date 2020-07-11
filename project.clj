(defproject biiwide/kvdb "0.1.0-SNAPSHOT"

  :description "A reusable protocol for KeyValue DataBases."

  :url "https://github.com/biiwide/kvdb"

  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}

  :plugins [[org.clojure/clojurescript "1.10.758"]
            [lein-doo "0.1.10"]]

  :cljsbuild {:builds
              {:test {:source-paths ["src" "test"]
                      :compiler {:output-to "target/unit-test.js"
                                 :optimizations :whitespace
                                 :pretty-print true}}}}

  :doo {:build "test"
        :alias {:default [:firefox]}}

  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/test.check "1.0.0"]]

  :repl-options {:init-ns biiwide.kvdb}

  :profiles {:dev {:dependencies [[org.clojure/clojurescript "1.10.758"]]}}

  )
