(ns biiwide.kvdb.protocols
  (:refer-clojure :exclude [key remove replace]))


(defprotocol KVDBEntry
  "All records returned by KVDB functions will satisfy the KVDBEntry protocol."
  :extend-via-metadata true
  (entry? [x])
  (key [entry])
  (value [entry])
  (revision [entry]))


(defprotocol KVDBable
  "The KVDBable protocol is supported by types that can be coerced into
a KVDB instance."
  :extend-via-metadata true
  (to-kvdb [x] "Convert a supported KVDB type into a full KVDB instance."))


(defprotocol ReadableKVDB
  "The KVDB protocol defines common operations for key/value databases."
  :extend-via-metadata true
  (readable-kvdb? [x] "Test if a value supports the ReadableKVDB protocol.")
  (fetch [kvdb k missing-value]
         "Retrieves the entry for a key from the database.
Returns 'misising-value when the key is not present.
Will only throw exceptions when unable to communicate with the database.")
  (entries [kvdb]
           "Returns a sequence of all key/value entries in the databse.
Will only throw exceptions when unable to communicate wth the database.")
  )


(defprotocol MutableKVDB
  "The KVDB protocol defines common operations for mutable key/value databases."
  :extend-via-metadata true
  (mutable-kvdb? [x]
    "Test if a value supports the MutableKVDB protocol.")
  (create! [kvdb key value]
    "Creates an entry in the database.
Throws an exception when the key is already present.
Returns the new entry.")
  (remove! [kvdb key revision]
    "Removes an entry from the database.
Asserts the entry's revision matches the provided revision.
Returns the final state of the entry.")
  (replace! [kvdb key revision new-value]
    "Replaces the value of an entry in the database with a new value.
Asserts the entry's revision matches the provided revision.
Returns the new state of the entry.")
  )


(defprotocol PageableKVDB
  "The PageableKVDB protocol provides a resumable mechanism for retrieving pages of database entries."
  :extend-via-metadata true
  (pageable-kvdb? [x]
    "Test if a value supports the PageableKVDB protocol.")
  (page [kvdb starting-key limit]
    "Returns a page of up to 'limit results starting from 'starting-key inclusively.")
  )


(defprotocol OverridableKVDB
  "The OverridableKVDB protocol provides support for overriding protocol implementations on a KVDB instance."
  (overridable-kvdb? [x]
    "Test if a value supports the OverridableKVDB protocol.")
  (push-overrides [kvdb implementations-map]
    "Overlay a set of overriden KVDB protocol methods on a KVDB instance.")
  (overridden [kvdb]
    "Return a map of all overridden KVDB methods on a KVDB instance.")
  (pop-overrides [kvdb]
    "Remove the prior set of overridden methods from an OverridableKVDB.")
  )
