(ns tech.v3.datatype
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.base :as dt-base]
            [tech.v3.datatype.copy-make-container :as dt-cmc])
  (:refer-clojure :exclude [clone counted?]))

(defn ecount
  [item]
  (if item
    ;;As count is a protocol in js, we have no reason to define our own
    (count item)
    0))


(defn clone
  "Here for compat with jvm system"
  [item]
  (cljs.core/clone item))


(defn elemwise-datatype
  [item]
  (if item
    (dtype-proto/-elemwise-datatype item)
    :object))


(defn datatype
  [item]
  (if item
    (dtype-proto/-datatype item)
    :object))

(defn as-typed-array
  [item]
  (dt-base/as-typed-array item))


(defn as-js-array
  [item]
  (dt-base/as-js-array item))


(defn sub-buffer-copy
  "Create a copy of the data in the item from offset till len."
  [item off & [len]]
  (dt-base/sub-buffer-copy item off len))


(defn counted?
  [item]
  (dt-base/counted? item))


(defn set-value!
  [item idx data]
  (dt-base/set-value! item idx data))


(defn copy!
  [src dest]
  (set-value! dest 0 src))


(defn make-container
  "Return a packed container of data.  Container implements count, nth, and meta,
  with-meta, and elemwise-datatype.  Uses typed-arrays for most primitive types and
  generic js arrays for anything else.  The container itself cannot check that the input
  matches the datatype so that form of checking is not available in the js version
  (unlike the jvm version)."
  [dtype len-or-data]
  (dt-cmc/make-container dtype len-or-data))
