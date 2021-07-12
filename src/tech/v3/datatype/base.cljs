(ns tech.v3.datatype.base
  (:require [tech.v3.datatype.protocols :as dtype-proto])
  (:refer-clojure :exclude [clone counted?]))


(defn as-typed-array
  [item]
  (when (and item (dtype-proto/-convertible-to-typed-array? item))
    (dtype-proto/->typed-array item)))


(defn as-js-array
  [item]
  (when (and item (dtype-proto/-convertible-to-js-array? item))
    (dtype-proto/->js-array item)))


(defn sub-buffer-copy
  "Create a copy of the data in the item from offset till len."
  [item off & [len]]
  (let [elen (count item)
        maxlen (- elen off)
        len (or len maxlen)]
    (when-not (>= elen (+ off len))
      (throw (js/Error. (str "Offset " off " len " len " => item length " elen))))
    (dtype-proto/-sub-buffer-copy item off len)))


(defn counted?
  [item]
  (when item
    (if (instance? js/Array item)
      true
      (cljs.core/counted? item))))


(defn ensure-indexable
  [data]
  (if-not (or (instance? js/Array data)
              (indexed? data))
    (vec data)
    data))


(defn as-agetable
  [data]
  (or (as-typed-array data) (as-js-array data)))


(defn set-value!
  [item idx data]
  (when-not item
    (throw (js/Error. "Item is nil")))
  (when-not (< idx (count item))
    (throw (js/Error. "Index is out of item length")))
  (when (and (counted? data)
             (not (<= (+ idx (count data)) (count item))))
    (throw (js/Error. (str "data length + idx " (+ (count data) idx)
                           " is out of range of item length ") (count item))))
  (dtype-proto/-set-value! item idx data)
  item)


(defn set-constant!
  [item idx elem-count data]
  (when-not item
    (throw (js/Error. "Item is nil")))
  (when-not (<= (+ idx elem-count) (count item))
    (throw (js/Error. "Index is out of item length")))
  (when (and (counted? data)
             (not (<= (+ idx (count data)) (count item))))
    (throw (js/Error. (str "data length + idx " (+ (count data) idx)
                           " is out of range of item length ") (count item))))
  (dtype-proto/-set-constant! item idx elem-count data)
  item)
