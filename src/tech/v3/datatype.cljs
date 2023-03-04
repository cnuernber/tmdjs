(ns tech.v3.datatype
  "Support for programming with arrays and a fast set implementation for indexe (int32) values.
  For complex/higher order algorithms see [[tech.v3.datatype.argops]].
  For mathematical primitives, see [[tech.v3.datatype.functional]]"
  (:require [tech.v3.datatype.protocols :as dt-proto]
            [tech.v3.datatype.base :as dt-base]
            [tech.v3.datatype.copy-make-container :as dt-cmc]
            [tech.v3.datatype.list :as dt-list]
            [tech.v3.datatype.arrays :as dt-arrays]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.bitmap :as bitmap]
            [tech.v3.datatype.reader-vec :as rvec]
            [tech.v3.datatype.emap1-vec :as emap1])
  (:refer-clojure :exclude [clone counted? indexed? reverse]))


(defn ecount
  "Compatibility with jvm-version.  All of the datatype objects
  implement ICounted so cljs.core/count works fine."
  [item]
  (dt-base/ecount item))


(defn shape
  [item]
  (dt-base/generalized-shape item))


(defn clone
  "Here for compat with jvm system.  All of the datatype objects
  implement ICloneable so cljs.core/clone works fine."
  [item]
  (dt-base/clone item))


(defn elemwise-datatype
  "Get the datatype of the elements in the container."
  [item]
  (dt-base/elemwise-datatype item))


(defn datatype
  "Get the dataytpe of this object."
  [item]
  (dt-base/datatype item))


(defn numeric-type?
  "Return true if this datatype is a numeric type.  True for
  :int8, :uint8 -> :int64, :uint64, float32, float64."
  [dtype]
  (when dtype (casting/numeric-type? dtype)))


(defn as-typed-array
  "Return the typed array data backing this container.  The object returned
  may have a different elemwise-datatype than the container."
  [item]
  (dt-base/as-typed-array item))


(defn as-js-array
  "Return the js array data backing this container.  The object returned
  may have a different elemwise-datatype than the container."
  [item]
  (dt-base/as-js-array item))


(defn ensure-indexable
  "Ensure this object is indexable.  If object is not indexable this calles
  `(vec item)`"
  [item]
  (dt-base/ensure-indexable item))


(defn as-agetable
  "Return something that you can safely call `(aget item idx)` and that
  will return data of the same datatype.  For `:int64` this may return
  `BigNum` objects."
  [item]
  (dt-base/as-agetable item))


(defn ->fast-nth
  "Given an arbitrary container, return the fastest idx->value accessor possible.
  If the item is agetable, this just wraps aget.  Else, unless otherwise instructed,
  it will wrap #(-nth item %)."
  [item]
  (dt-proto/->fast-nth item))


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
  "Return true if this is a clojure integer range object."
  [item]
  (dt-base/integer-range? item))


(defn iterate!
  "Efficiently iterate through the data calling consume-fn on every
  item.

  Returns consume-fn."
  [consume-fn data]
  (dt-base/iterate! consume-fn data))


(defn indexed-iterate!
  "Efficiently iterate through the data calling consum-fn and passing
  in an index for every item.

  Returns consume-fn."
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
  "Return true of js arrays and anything implementing ICounted."
  [item]
  (dt-base/counted? item))


(defn indexed?
  "Return true for js arrays and anything implementing IIndexed."
  [item]
  (dt-base/indexed? item))


(defn set-value!
  "Set value on the item.  data may be a scalar or a sequence or
  Indexable object in which case it will be copied into item
  starting at idx.

  Returns item"
  [item idx data]
  (dt-base/set-value! item idx data))


(defn set-constant!
  "Set a constant value starting at idx and continuing elem-count values."
  [item idx elem-count value]
  (dt-base/set-constant! item idx elem-count value))


(defn copy!
  "Copy src container into dest container."
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
  "Ensure that itme is a typed array.  Only works for numeric datatypes."
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
  `add!`, `add-all!`, `ensure-capacity!`"
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


(defn list-coalesce!
  "Coalesce a possibly nested set of data into a single container that implements the
  dtype list protocol.

  Example:

```clojure
cljs.user> (dtype/list-coalesce! [[2 3 4][5 6 7]] (dtype/make-list :float32))
#list[[:float32 6][2 3 4 5 6 7]
```"
  [data container]
  (dt-base/list-coalesce! data container))


(defn ->js-set
  "Create a javascript set.  These have superior performance when dealing with numeric
  data but they fail completely when dealing with clojure data such as persistent maps
  or keywords"
  ([] (bitmap/->js-set))
  ([data] (bitmap/->js-set data)))


(defn ->set
  "Convert arbitrary data into a set appropriate for the data.  For numeric types,
  javascript sets are fastest else use a clojure set."
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
  "Create a predicate out of a set.  For js sets, calls .has and for
  Clojure sets just returns the set object."
  [set]
  (cond
    (instance? js/Set set)
    #(.has set %)
    (set? set) set
    :else
    (throw (js/Error. "Item passed in is not a js set nor a cljs.core set."))))


(defn set-predicate-complement
  "Create a not-in-set predicate.  See docs for [[set-predicate]]"
  [set]
  (cond
    (instance? js/Set set)
    #(not (.has set %))
    (set? set)
    #(not (set %))
    :else
    (throw (js/Error. "Item passed in is not a js set nor a cljs.core set."))))


(defn indexed-buffer
  "Reindex the buffer via the given indexes returning a new buffer.  Unlike the jvm version
  this most likely results in a new container."
  [indexes data]
  (dt-arrays/indexed-buffer indexes data))


(defn reify-reader
  "Create a new reader of elemwise-datatype dtype.  The returned reader correctly
  implements hash and equiv such that it is indistinguishable from a persistent
  vector created as `(mapv idx->val-fn (range n-elems))`"
  ([dtype n-elems idx->val-fn]
   (rvec/reader-vec n-elems dtype idx->val-fn))
  ([n-elems idx->val-fn]
   (reify-reader :object n-elems idx->val-fn)))


(defn reverse
  [item]
  (cond
    (empty? item) item
    (dt-base/integer-range? item)
    (let [ec (ecount item)
          start (aget item "start")
          step (aget item "step")
          end (+ start (* step ec))]
      (range (- end step) (- start step) (- step)))
    (dt-base/indexed? item)
    (let [ec (ecount item)
          dec-ec (dec ec)]
      (reify-reader (elemwise-datatype item) ec
                    (if-let [aget-item (dt-base/as-agetable item)]
                      #(aget aget-item (- dec-ec %))
                      #(nth item (- dec-ec %)))))
    :else
    (cljs.core/reverse item)))


(defn emap
  "elementwise map a function over a sequences.  Returns a countable/indexable array
  of results."
  [map-fn ret-dtype & args]
  (when (= 0 (count args))
    (throw (js/Error. "No args provided, not a transducing function")))
  (let [ret-dtype (or ret-dtype (reduce casting/widest-datatype
                                        (map elemwise-datatype args)))]
    (if (every? indexed? args)
      (let [n-elems (apply min (map count args))]
        (case (count args)
          1 (emap1/emap1-vec ret-dtype map-fn (first args))
          2 (let [arg1 (first args)
                  arg2 (second args)
                  aa1 (dt-base/as-agetable arg1)
                  aa2 (dt-base/as-agetable arg2)]
              ;;Using aget when possible is a factor of 10 in summation times
              (reify-reader ret-dtype n-elems
                            (if (and aa1 aa2)
                              #(map-fn (aget aa1 %)
                                       (aget aa2 %))
                              #(map-fn (nth arg1 %)
                                       (nth arg2 %)))))
          3 (let [arg1 (nth args 0)
                  arg2 (nth args 1)
                  arg3 (nth args 2)]
              (reify-reader ret-dtype n-elems #(map-fn (nth arg1 %)
                                                       (nth arg2 %)
                                                       (nth arg3 %))))
          (reify-reader ret-dtype
                        n-elems
                        (fn [idx] (apply map-fn (map #(nth % idx) args))))))
      (if (= ret-dtype :object)
        (apply map map-fn args)
        (reify
          ISeqable
          (-seq [r]
            (when (every? seq args)
              (apply map map-fn args)))
          dt-proto/PElemwiseDatatype
          (-elemwise-datatype [this] ret-dtype))))))
