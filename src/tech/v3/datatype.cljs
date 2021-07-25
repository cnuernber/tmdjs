(ns tech.v3.datatype
  "Support for programming with arrays and a fast set implementation for indexe (int32) values.
  For complex/higher order algorithms see [[tech.v3.datatype.argops]]"
  (:require [tech.v3.datatype.protocols :as dt-proto]
            [tech.v3.datatype.base :as dt-base]
            [tech.v3.datatype.copy-make-container :as dt-cmc]
            [tech.v3.datatype.list :as dt-list]
            [tech.v3.datatype.arrays :as dt-arrays]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.bitmap :as bitmap]
            [tech.v3.datatype.reader-vec :as rvec]
            [clojure.string :as s])
  (:refer-clojure :exclude [clone counted? indexed?]))


(defn ecount
  [item]
  ;;just calls cljs.clone
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


(defn numeric-type?
  [dtype]
  (when dtype (casting/numeric-type? dtype)))


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


(defn as-datatype-accurate-agetable
  "Only reduce to an aget-able item of the datatypes match.  This avoids
  throwing away js array wrappers that may do more checking and that can
  accurately advertise elemwise datatypes."
  [item]
  (when-let [aget-item (as-agetable item)]
    (when (= (elemwise-datatype item)
             (elemwise-datatype aget-item))
      aget-item)))


(defn integer-range?
  [item]
  (dt-base/integer-range? item))


(defn iterate!
  [consume-fn data]
  (dt-base/iterate! consume-fn data))


(defn indexed-iterate!
  [consume-fn data]
  (dt-base/indexed-iterate! consume-fn data))


(defn sub-buffer-copy
  "Create a copy of the data in the item from offset till len."
  [item off & [len]]
  (dt-base/sub-buffer-copy item off len))


(defn sub-buffer
  "Create a sub-buffer that in most cases shared the backing store data with the parent
  buffer"
  [item off & [len]]
  (dt-base/sub-buffer item off len))


(defn counted?
  [item]
  (dt-base/counted? item))


(defn indexed?
  [item]
  (dt-base/indexed? item))


(defn set-value!
  [item idx data]
  (dt-base/set-value! item idx data))


(defn set-constant!
  [item idx elem-count value]
  (dt-base/set-constant! item idx elem-count value))


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


(defn ensure-typed-array
  [item]
  (let [item-dt (elemwise-datatype item)]
    (when-not (numeric-type? item-dt)
      (throw (js/Error. (str "Data is not a numeric datatype: " item-dt))))
    (if-let [retval (or (as-typed-array item)
                        (as-typed-array (make-container item-dt item)))]
      retval
      (throw (js/Error. "Unable to convert data to a typed array")))))


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


(defn ensure-capacity!
  "Ensure the list has at least this much capacity without changing the number of
  inserted items"
  [list buflen]
  (dt-proto/-ensure-capacity list buflen))


(defn- maybe-min-count
  [arg-seq]
  (let [farg (first arg-seq)]
    (when (counted? farg)
      (reduce (fn [min-c arg]
                (when (and min-c (counted? arg))
                  (min min-c (count arg))))
              (count farg)
              (rest arg-seq)))))


(defn ->js-set
  "Create a javascript set.  These have superior performance when dealing with numeric
  data but they fail completely when dealing with clojure data such as persistent maps
  or keywords"
  ([] (bitmap/->js-set))
  ([data] (bitmap/->js-set data)))


(defn ->set
  [data]
  (cond
    (or (set? data)
        (instance? js/Set data))
    data
    (nil? data)
    (set nil)
    (numeric-type? (elemwise-datatype data))
    (->js-set data)
    :else
    (set data)))


(defn set-or
  "Perform set-union. Implemented for efficiently js sets and clojure sets."
  [lhs rhs] (dt-proto/-set-or lhs rhs))

(defn set-and
  "Perform set-intersection.  Implemented for efficiently js sets and clojure sets."
  [lhs rhs] (dt-proto/-set-and lhs rhs))


(defn set-and-not
  "Perform set-difference.  Implemented for efficiently js sets and clojure sets."
  [lhs rhs] (dt-proto/-set-and-not lhs rhs))


(defn set-offset
  "Offset every member of the set by a constant returning a new set"
  [lhs off]
  (dt-proto/-set-offset lhs off))


(defn set->ordered-indexes
  "Create an ordered int32 buffer of the items in the set."
  [data]
  (bitmap/set->ordered-indexes data))


(defn set-predicate
  [set]
  (if (instance? js/Set set)
    #(.has set %)
    set))

(defn set-predicate-complement
  [set]
  (if (instance? js/Set set)
    #(not (.has set %))
    #(not (set %))))


(defn indexed-buffer
  "Reindex the buffer via the given indexes returning a new buffer."
  [indexes data]
  (dt-arrays/indexed-buffer indexes data))


(defn reify-reader
  ([n-elems dtype idx->val-fn]
   (rvec/reader-vec n-elems dtype idx->val-fn))
  ([n-elems idx->val-fn]
   (reify-reader n-elems :object idx->val-fn)))


(defn emap
  "elementwise map a function over a sequences.  Returns a countable/indexable array
  of results."
  [map-fn ret-dtype & args]
  (if (= 0 (count args))
    (throw (js/Error. "No args provided, not a transducing function")))
  (let [ret-dtype (or ret-dtype (reduce casting/widest-datatype
                                        (map elemwise-datatype args)))]
    (if (every? indexed? args)
      (let [n-elems (apply min (map count args))]
        (case (count args)
          1  (let [arg (first args)
                   aarg (dt-base/as-agetable arg)]
               (reify-reader n-elems ret-dtype (if aarg
                                                 #(map-fn (aget aarg %))
                                                 #(map-fn (nth arg %)))))
          2 (let [arg1 (first args)
                  arg2 (second args)]
              (reify-reader n-elems ret-dtype #(map-fn (nth arg1 %)
                                                       (nth arg2 %))))
          3 (let [arg1 (nth args 0)
                  arg2 (nth args 1)
                  arg3 (nth args 3)]
              (reify-reader n-elems ret-dtype #(map-fn (nth arg1 %)
                                                       (nth arg2 %)
                                                       (nth arg3 %))))
          (reify-reader n-elems
                        ret-dtype
                        (fn [idx] (apply map-fn (map #(nth % idx) args))))))
      (if (= ret-dtype :object)
        (apply map map-fn args)
        (reify
          ISeqable
          (-seq [r] (apply map map-fn args))
          dt-proto/PElemwiseDatatype
          (-elemwise-datatype [this] ret-dtype))))))
