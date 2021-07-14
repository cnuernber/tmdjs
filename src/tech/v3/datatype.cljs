(ns tech.v3.datatype
  (:require [tech.v3.datatype.protocols :as dt-proto]
            [tech.v3.datatype.base :as dt-base]
            [tech.v3.datatype.copy-make-container :as dt-cmc]
            [tech.v3.datatype.list :as dt-list]
            [tech.v3.datatype.arrays :as dt-arrays]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.bitmap :as bitmap])
  (:refer-clojure :exclude [clone counted?]))


(defn ecount
  [item]
  (dt-base/ecount item))


(defn clone
  "Here for compat with jvm system"
  [item]
  (dt-base/clone item))


(defn elemwise-datatype
  [item]
  (dt-base/elemwise-datatype item))


(defn datatype
  [item]
  (dt-base/datatype item))


(defn as-typed-array
  [item]
  (dt-base/as-typed-array item))


(defn as-js-array
  [item]
  (dt-base/as-js-array item))


(defn ensure-indexable
  [item]
  (dt-base/ensure-indexable item))


(defn as-agetable
  [item]
  (dt-base/as-agetable item))


(defn integer-range?
  [item]
  (dt-base/integer-range? item))


(defn iterate-range!
  [consume-fn range]
  (dt-base/iterate-range! consume-fn range))


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


(defn make-list
  "Make a list.  Lists implement the tech.v3.datatype.protocols/PListLike protocol -
  `-add`, `-add-all`"
  [dtype & [init-buf-size]]
  (dt-list/make-list dtype init-buf-size))


(defn add!
  "Add an item to a list."
  [list item]
  (dt-proto/-add list item)
  list)


(defn add-all!
  "add a countable sequence of items to a list"
  [list items]
  (dt-proto/-add-all list items)
  list)


(defn- maybe-min-count
  [arg-seq]
  (let [farg (first arg-seq)]
    (when (counted? farg)
      (reduce (fn [min-c arg]
                (when (and min-c (counted? arg))
                  (min min-c (count arg))))
              (count farg)
              (rest arg-seq)))))


(defn- emap-list
  [map-fn ret-dtype args]
  (let [retval (dt-list/make-primitive-list
                (make-container ret-dtype
                                (or (maybe-min-count args) 8))
                ret-dtype 0)]
    (doseq [item (apply map map-fn args)]
      (dt-proto/-add retval item))
    retval))


(defn emap
  "non-lazy elementwise map a function over a sequences.  Returns a countable/indexable array
  of results."
  [map-fn ret-dtype & args]
  (let [ret-dtype (or ret-dtype (reduce casting/widest-datatype (map elemwise-datatype args)))]
    (if (= 1 (count args))
      ;;special case out of we can use array.map or typed-array.map
      (let [arg (first args)
            aarg (dt-base/as-agetable arg)]
        (if (and aarg (= ret-dtype (elemwise-datatype arg)))
          (dt-arrays/make-typed-buffer (.map aarg map-fn) ret-dtype)
          (emap-list map-fn ret-dtype args)))
      (emap-list map-fn ret-dtype args))))


(defn ->js-set
  "Create a javascript set.  These have superior performance when dealing with int32 indexes."
  ([] (bitmap/->js-set))
  ([data] (bitmap/->js-set data)))


(defn set-or
  "Perform set-union. Implemented for efficiently js sets and clojure sets."
  [lhs rhs] (dt-proto/-set-or lhs rhs))

(defn set-and
  "Perform set-intersection.  Implemented for efficiently js sets and clojure sets."
  [lhs rhs] (dt-proto/-set-and lhs rhs))


(defn set-and-not
  "Perform set-difference.  Implemented for efficiently js sets and clojure sets."
  [lhs rhs] (dt-proto/-set-and-not lhs rhs))
