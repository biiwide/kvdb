(defproject biiwide/kvdb-api "0.1.2-SNAPSHOT"

  :description "A reusable protocol for KeyValue DataBases."

  :url "https://github.com/biiwide/kvdb"

  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}

  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/test.check "1.1.0" :scope "provided"]]

  :repl-options {:init-ns biiwide.kvdb}
  )
