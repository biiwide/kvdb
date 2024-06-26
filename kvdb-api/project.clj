(defproject biiwide/kvdb-api "0.1.5-SNAPSHOT"

  :description "A reusable protocol for KeyValue DataBases."

  :url "https://github.com/biiwide/kvdb"

  :license {:name "EPL-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}

  :plugins [[lein-ancient "1.0.0-RC3"]]

  :dependencies [[org.clojure/clojure "1.10.3" :scope "provided"]
                 [org.clojure/test.check "1.1.1" :scope "provided"]]

  :repl-options {:init-ns biiwide.kvdb}
  )
