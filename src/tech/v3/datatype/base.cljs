(ns tech.v3.datatype.base
  (:require [tech.v3.datatype.protocols :as dt-proto]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.format-sequence :as fmt]
            [tech.v3.datatype.argtypes :as argtypes]
            [ham-fisted.api :as hamf]
            [goog.object :as gobject]
            [clojure.string :as s])
  (:refer-clojure :exclude [clone counted? indexed?]))


(defn ecount
  [item]
  (if (nil? item)
    nil
    ;;As count is a protocol in js, we have no reason to define our own
    (count item)))


(defn shape
  [item]
  (if (nil? item) nil (dt-proto/-shape item)))


(defn clone
  "Here for compat with jvm system"
  [item]
  (cljs.core/clone item))


(defn elemwise-datatype
  [item]
  (if (nil? item)
    :object
    (dt-proto/-elemwise-datatype item)))


(defn datatype
  [item]
  (if (nil? item)
    :object
    (dt-proto/-datatype item)))


(defn as-typed-array
  [item]
  (when (and item (dt-proto/-convertible-to-typed-array? item))
    (dt-proto/->typed-array item)))


(defn as-js-array
  [item]
  (when (and item (dt-proto/-convertible-to-js-array? item))
    (dt-proto/->js-array item)))


(defn sub-buffer-copy
  "Create a copy of the data in the item from offset till len."
  [item off & [len]]
  (let [elen (count item)
        maxlen (- elen off)
        len (or len maxlen)]
    (when-not (>= elen (+ off len))
      (throw (js/Error. (str "Offset " off " len " len " => item length " elen))))
    (dt-proto/-sub-buffer-copy item off len)))


(defn sub-buffer
  [item off & [len]]
  (let [elen (count item)
        maxlen (- elen off)
        len (or len maxlen)]
    (when-not (>= elen (+ off len))
      (throw (js/Error. (str "Offset " off " len " len " => item length " elen))))
    (dt-proto/-sub-buffer item off len)))


(defn counted?
  [item]
  (when item
    (if (instance? js/Array item)
      true
      (cljs.core/counted? item))))


(defn indexed?
  [item]
  (when item
    (if (instance? js/Array item)
      true
      (cljs.core/indexed? item))))


(defn ensure-indexable
  [data]
  (if-not (or (instance? js/Array data)
              (indexed? data))
    (vec data)
    data))


(defn as-agetable
  [data]
  (when (and data (dt-proto/-convertible-to-agetable? data))
    (dt-proto/->agetable data)))


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
  (dt-proto/-set-value! item idx data)
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
  (dt-proto/-set-constant! item idx elem-count data)
  item)


(defn integer-range?
  [item]
  (or (instance? IntegerRange item)
      (and (instance? hamf/RangeType item)
           (.isInteger ^JS item))))


(defn- consumer-acc
  [acc v]
  (acc v) acc)


(defn as-js-iterator
  [data]
  (when data
    (when-let [iter-fn (gobject/get data ITER_SYMBOL)]
      (.call iter-fn data))))


(defn as-iterator
  [data]
  (when (and data (satisfies? IIterable data))
    (-iterator data)))


(defn indexed-iterate!
  [consume-fn item]
  (reduce (hamf/indexed-accum-fn
           (fn [acc idx v]
             (acc idx v)
             acc))
          consume-fn item))


(defn iterate!
  [consume-fn item]
  (reduce consumer-acc consume-fn item))


(defn reader-data->str
  ([rdr dtype]
   ;;numpy-style  first/last print of longer sequences
   (let [n-elems (count rdr)
         ellipses? (> n-elems 25)
         n-shortened 10
         rdr-data (if ellipses?
                    (concat (sub-buffer rdr 0 n-shortened)
                            (sub-buffer rdr (- n-elems n-shortened) n-shortened))
                    rdr)
         formatted (if (casting/numeric-type? dtype)
                     (fmt/format-sequence rdr-data)
                     rdr-data)]

     (if ellipses?
       (s/join " " (concat (take n-shortened formatted)
                           ["..."]
                           (drop n-shortened formatted)))
       (s/join " " formatted))))
  ([rdr]
   (reader-data->str rdr (elemwise-datatype rdr))))


(defn reader->str
  [rdr typename]
  (let [simple-print? (get (meta rdr) :simple-print?)
        cnt (count rdr)
        dtype (elemwise-datatype rdr)
        rdr-str (reader-data->str rdr dtype)]
    (if simple-print?
      (str "[" rdr-str "]")
      (str "#" typename "[[" dtype " " cnt "][" rdr-str "]]"))))


(defn list-coalesce!
  "Coalesce data into a container that implements add! and add-all!.  Returns the container."
  [data container]
  (if (= :scalar (argtypes/argtype data))
    (dt-proto/-add container data)
    (if (= :scalar (argtypes/argtype (first data)))
      (dt-proto/-add-all container data)
      (iterate! #(list-coalesce! % container) data)))
  container)


(defn generalized-shape
  [data]
  (cond
    (or (nil? data) (= :scalar (argtypes/argtype data)))
    []
    (satisfies? dt-proto/PShape data)
    (dt-proto/-shape data)
    :else
    (loop [shp (transient [(count data)])
           data (first data)]
      (if (= :scalar (argtypes/argtype data))
        (persistent! shp)
        (recur (conj! shp (count data)) (first data))))))


(extend-type cljs.core/PersistentVector
  dt-proto/PSubBuffer
  (-sub-buffer [item offset len]
    (subvec item offset (+ offset len))))
