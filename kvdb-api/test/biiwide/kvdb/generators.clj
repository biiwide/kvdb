(ns biiwide.kvdb.generators
  (:refer-clojure :exclude [key])
  (:require [biiwide.kvdb :as kvdb]
            [biiwide.kvdb.atomic :as atomic]
            [clojure.test.check.generators :as gen]
            [clojure.walk :as walk]))


(def key
 (constantly gen/string))

(def value
  (gen/map gen/simple-type-printable-equatable
           gen/any-printable-equatable))

