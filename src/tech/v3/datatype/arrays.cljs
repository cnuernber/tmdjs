(ns tech.v3.datatype.arrays
  (:require [tech.v3.datatype.protocols :as dt-proto]
            [tech.v3.datatype.base :as dt-base]
            [tech.v3.datatype.argtypes :as argtypes])
  (:refer-clojure :exclude [make-array]))

(set! *unchecked-arrays* true)

(def ary-types
  {js/Int8Array :int8
   js/Uint8Array :uint8
   js/Uint8ClampedArray :uint8
   js/Int16Array :int16
   js/Uint16Array :uint16
   js/Int32Array :int32
   js/Uint32Array :uint32
   js/Float32Array :float32
   js/Float64Array :float64})

(def typed-array-types (set (map second ary-types)))


(defn hash-next
  [hashcode nexthash]
  (bit-or (+ (imul 31 hashcode) nexthash) 0))


(defn hash-agetable
  [item]
  (let [item (dt-base/as-agetable item)
        n-elems (count item)]
    (loop [idx 0
           hash-code 1]
      (if (< idx n-elems)
        (recur (unchecked-inc idx)
               (bit-or (+ (imul 31 hash-code) (hash (aget item idx))) 0))
        (mix-collection-hash hash-code n-elems)))))


(defn hash-nthable
  [item]
  (let [n-elems (count item)]
    (loop [idx 0
           hash-code 1]
      (if (< idx n-elems)
        (recur (unchecked-inc idx)
               (bit-or (+ (imul 31 hash-code) (hash (nth item idx))) 0))
        (mix-collection-hash hash-code n-elems)))))


(defn equiv-agetable
  [this other]
  (let [this (dt-base/as-agetable this)
        n-elems (count this)]
    ;;ecount accounts for nil
    (if (and this (== n-elems (dt-base/ecount other)))
      (if-let [other (dt-base/as-agetable other)]
        (loop [idx 0]
          (if (< idx n-elems)
            (if (= (aget this idx)
                   (aget other idx))
              (recur (inc idx))
              false)
            true))
        (loop [idx 0]
          (if (< idx n-elems)
            (if (= (aget this idx)
                   (nth other idx))
              (recur (inc idx))
              false)
            true)))
      false)))


(defn equiv-nthable
  [this other]
  (let [n-elems (count this)]
    ;;ecount accounts for nil
    (if (and this (== n-elems (dt-base/ecount other)))
      (if-let [other (dt-base/as-agetable other)]
        (loop [idx 0]
          (if (< idx n-elems)
            (if (= (nth this idx)
                   (aget other idx))
              (recur (inc idx))
              false)
            true))
        (loop [idx 0]
          (if (< idx n-elems)
            (if (= (nth this idx)
                   (nth other idx))
              (recur (inc idx))
              false)
            true)))
      false)))


(deftype AgetIter [data n-elems ^:unsynchronized-mutable i]
  Object
  (hasNext [this]
    (< i n-elems))
  (next [this]
    (let [ret (aget data i)]
      (set! i (inc i))
      ret)))

(defn aget-iter
  [data]
  (if-let [data (dt-base/as-agetable data)]
    (AgetIter. data (count data) 0)
    (throw (js/Error. "Data is not agetable"))))


(deftype NthIter [data n-elems ^:unsynchronized-mutable i]
  Object
  (hasNext [this]
    (< i n-elems))
  (next [this]
    (let [ret (nth data i)]
      (set! i (inc i))
      ret)))

(defn nth-iter
  [data]
  (NthIter. data (count data) 0))


(defn index-iter
  [data]
  (if-let [data (dt-base/as-agetable data)]
    (aget-iter data)
    (nth-iter data)))


(doseq [ary-type (map first ary-types)]
  (extend-type ary-type
    dt-proto/PElemwiseDatatype
    (-elemwise-datatype [item] (ary-types ary-type))
    dt-proto/PDatatype
    (-datatype [item] :typed-array)
    dt-proto/PToTypedArray
    (-convertible-to-typed-array? [item] true)
    (->typed-array [item] item)
    dt-proto/PSubBufferCopy
    (-sub-buffer-copy [item off len]
      (.slice item off (+ off len)))
    dt-proto/PSubBuffer
    (-sub-buffer [item off len]
      (.subarray item off (+ off len)))
    IHash
    (-hash [o] (hash-agetable o))
    IEquiv
    (-equiv [this other]
      (equiv-agetable this other))
    ICloneable
    (-clone [item]
      (let [len (aget item "length")
            retval (js* "new item.constructor(len)")]
        (.set retval item)
        retval))
    ISequential
    ISeqable
    (-seq [array] (array-seq array))
    ISeq
    (-first [array] (aget array 0))
    (-rest  [array] (.subarray array 1))
    IIndexed
    (-nth
      ([array n]
       (aget array n))
      ([array n not-found]
       (if (< n (count array))
         (aget array n)
         not-found)))
    ICounted
    (-count [array] (.-length array))
    IReduce
    (-reduce
      ([array f] (array-reduce array f))
      ([array f start] (array-reduce array f start)))
    IPrintWithWriter
    (-pr-writer [rdr writer opts]
      (-write writer (dt-base/reader->str rdr "typed-array")))
    dt-proto/PSetValue
    (-set-value! [item idx data]
      (cond
        (number? data)
        (aset item idx data)
        (dt-proto/-convertible-to-typed-array? data)
        (.set item (dt-proto/->typed-array data) idx)
        (dt-proto/-convertible-to-js-array? data)
        (let [data (dt-proto/->js-array data)]
          (dotimes [didx (count data)]
            (aset item (+ idx didx) (aget data didx))))
        ;;common case for integer ranges
        (dt-base/integer-range? data)
        (if (and (= 1 (aget data "step"))
                 (= 0 (aget data "start")))
          (dotimes [ridx (count data)]
            (aset item (+ ridx idx) ridx))
          (dt-base/indexed-iterate-range! #(aset item (+ idx %1) %2) data))
        :else
        (dotimes [didx (count data)]
          (aset item (+ idx didx) (nth data didx))))
      item)
    dt-proto/PSetConstant
    (-set-constant! [item offset elem-count data]
      (.fill item data offset (+ offset elem-count)))))



(extend-type array
  dt-proto/PDatatype
  (-datatype [item] :js-array)
  dt-proto/PToJSArray
  (-convertible-to-js-array? [buf] true)
  (->js-array [buf] buf)
  dt-proto/PSubBufferCopy
  (-sub-buffer-copy [item off len]
    (.slice item off (+ off len)))
  dt-proto/PSubBuffer
  (-sub-buffer [item off len]
    ;;js arrays, like jvm arrays, have no sub-array functionality
    (.slice item off (+ off len)))
  ICloneable
  (-clone [item] (.slice item 0 (count item)))
  IHash
  (-hash [o] (hash-agetable o))
  IEquiv
  (-equiv [this other]
    (equiv-agetable this other))
  dt-proto/PSetValue
  (-set-value! [item idx data]
    (cond
      (number? data)
      (aset item idx data)
      (dt-base/as-agetable data)
      (let [data (dt-base/as-agetable data)]
        (dotimes [didx (count data)]
          (aset item (+ idx didx) (aget data didx))))
      (dt-base/integer-range? data)
      (let [start (aget data "start")
            step (aget data "step")
            rend (aget data "end")
            startpos (if (> step 0) start rend)
            n-elems (count data)]
        (if (and (= 0 start) (= 1 step))
          (dotimes [idx n-elems]
            (aset item idx idx))
          (dotimes [idx n-elems]
            (aset item idx (+ startpos (* idx step))))))
      :else
      (dotimes [didx (count data)]
        (aset item (+ idx didx) (nth data didx))))
    item)
  dt-proto/PSetConstant
  (-set-constant! [item offset elem-count data]
    (.fill item data offset (+ offset elem-count))))


(defn bool-val->byte
  [val]
  (if (number? val)
    (if (== 0.0 val) 0 1)
    (if val 1 0)))


(defn- booleans->bytes
  [data]
  (cond
    (argtypes/scalar? data) (bool-val->byte data)
    (dt-proto/-convertible-to-typed-array? data)
    (.map (dt-proto/->typed-array data) bool-val->byte)
    (dt-proto/-convertible-to-js-array? data)
    (.map (dt-proto/->js-array data) bool-val->byte)
    (sequential? data)
    (mapv bool-val->byte data)
    ;;scalars should fall through here.
    :else
    (if data 1 0)))


(defn byte->boolean
  [val]
  (if (== 0 val) false true))


(declare make-boolean-array)

;;Booleans are stored as 1,0 bytes.
(deftype BooleanArray [buf metadata ^:unsynchronized-mutable hashcode]
  ICounted
  (-count [item] (count buf))
  ICloneable
  (-clone [item] (make-boolean-array (clone buf) metadata))
  dt-proto/PElemwiseDatatype
  (-elemwise-datatype [item] :boolean)
  dt-proto/PSubBufferCopy
  (-sub-buffer-copy [item off len]
    (make-boolean-array (dt-proto/-sub-buffer-copy buf off len) metadata))
  dt-proto/PSubBuffer
  (-sub-buffer [item off len]
    (make-boolean-array (dt-proto/-sub-buffer buf off len) metadata))
  dt-proto/PSetValue
  (-set-value! [item idx data]
    (dt-proto/-set-value! buf idx (booleans->bytes data))
    item)
  dt-proto/PSetConstant
  (-set-constant! [item offset elem-count data]
    (dt-proto/-set-constant! buf offset elem-count
                                (booleans->bytes data))
    item)
  IHash
  (-hash [o]
    (when-not hashcode
      (set! hashcode (hash-nthable o)))
    hashcode)
  IEquiv
  (-equiv [this other]
    (equiv-nthable this other))
  IIterable
  (-iterator [this] (nth-iter this))
  dt-proto/PToTypedArray
  (-convertible-to-typed-array? [this] true)
  (->typed-array [this] buf)
  ;;Disable aget for this buffer.  This is because it will result in algorithms
  ;;getting the base buffer which will mean they get 1,0 instead of true,false.
  dt-proto/PAgetable
  (-convertible-to-agetable? [this] false)
  IWithMeta
  (-with-meta [coll new-meta]
    (if (identical? new-meta metadata)
      coll
      (BooleanArray. buf new-meta nil)))
  IMeta
  (-meta [coll] metadata)
  IPrintWithWriter
  (-pr-writer [rdr writer opts]
    (-write writer (dt-base/reader->str rdr "typed-array")))
  ISequential
  ISeqable
  (-seq [array] (map byte->boolean buf))
  ISeq
  (-first [array] (byte->boolean (nth buf 0)))
  (-rest  [array] (dt-base/sub-buffer array 1 (dec (count buf))))
  IFn
  (-invoke [array n]
    (let [n (if (< n 0) (+ (count array) n) n)]
      (byte->boolean (nth buf n))))
  IIndexed
  (-nth [array n]
    (let [n (if (< n 0) (+ (count array) n) n)]
      (byte->boolean (nth buf n))))
  (-nth [array n not-found]
    (let [n (if (< n 0) (+ (count array) n) n)]
      (if (< n (count buf))
        (byte->boolean (nth buf n))
        not-found))))


(defn make-boolean-array
  [buf & [metadata]]
  (BooleanArray. buf metadata nil))


(declare make-typed-buffer)


;;Necessary to add an actual datatype to a js array and metadata to typed arrays
(deftype TypedBuffer [buf elem-dtype metadata ^:unsynchronized-mutable hashcode]
  ICounted
  (-count [item] (count buf))
  ICloneable
  (-clone [item] (make-typed-buffer (clone buf) elem-dtype metadata))
  dt-proto/PElemwiseDatatype
  (-elemwise-datatype [item] elem-dtype)
  dt-proto/PToJSArray
  (-convertible-to-js-array? [item] (dt-proto/-convertible-to-js-array? buf))
  (->js-array [item] (dt-proto/->js-array buf))
  dt-proto/PToTypedArray
  (-convertible-to-typed-array? [item] (dt-proto/-convertible-to-typed-array? buf))
  (->typed-array [item] (dt-proto/->typed-array buf))
  dt-proto/PSubBufferCopy
  (-sub-buffer-copy [item off len]
    (make-typed-buffer (dt-base/sub-buffer-copy buf off len) elem-dtype metadata))
  dt-proto/PSubBuffer
  (-sub-buffer [item off len]
    (make-typed-buffer (dt-base/sub-buffer buf off len) elem-dtype metadata))
  dt-proto/PSetValue
  (-set-value! [item idx data]
    (dt-proto/-set-value! buf idx data)
    item)
  dt-proto/PSetConstant
  (-set-constant! [item offset elem-count data]
    (dt-proto/-set-constant! buf offset elem-count data)
    item)
  IWithMeta
  (-with-meta [coll new-meta]
    (if (identical? new-meta metadata)
      coll
      (make-typed-buffer buf elem-dtype new-meta)))
  IMeta
  (-meta [coll] metadata)
  IPrintWithWriter
  (-pr-writer [rdr writer opts]
    (-write writer (dt-base/reader->str rdr "typed-buffer")))
  ISeqable
  (-seq [array] (array-seq buf))
  ISeq
  (-first [array] (nth buf 0))
  (-rest  [array] (dt-proto/-sub-buffer buf 1 (dec (count buf))))
  IFn
  (-invoke [array n]
    (let [n (if (< n 0) (+ (count array) n) n)]
      (nth buf n)))
  IIndexed
  (-nth [array n]
    (let [n (if (< n 0) (+ (count array) n) n)]
      (nth buf n)))
  (-nth [array n not-found]
    (let [n (if (< n 0) (+ (count array) n) n)]
      (if (< n (count buf))
        (nth buf n)
        not-found)))
  ISequential
  IHash
  (-hash [o]
    (when-not hashcode
      (set! hashcode
            (if-let [aget-buf (dt-base/as-agetable buf)]
              (hash-agetable aget-buf)
              (hash-nthable buf))))
    hashcode)
  IEquiv
  (-equiv [this other]
    (if-let [aget-buf (dt-base/as-agetable buf)]
      (equiv-agetable aget-buf other)
      (equiv-nthable buf other)))
  IIterable
  (-iterator [this] (index-iter buf)))


(defn make-typed-buffer
  [buf & [dtype metadata]]
  (let [dtype (or dtype (dt-base/elemwise-datatype buf))]
    (TypedBuffer. buf dtype metadata nil)))

;;Shorthand as this is very common
(defn tbuf [item] (make-typed-buffer item))


(defn make-array
  [dtype len]
  (if (= dtype :boolean)
    (make-boolean-array (js/Int8Array.  len) nil)
    (-> (case dtype
          :int8 (js/Int8Array. len)
          :uint8 (js/Uint8Array. len)
          :int16 (js/Int16Array. len)
          :uint16 (js/Uint16Array. len)
          :int32 (js/Int32Array. len)
          :uint32 (js/Uint32Array. len)
          :float32 (js/Float32Array. len)
          :float64 (js/Float64Array. len)
          (js/Array. len))
        (make-typed-buffer dtype))))


(extend-type IntegerRange
  dt-proto/PElemwiseDatatype
  (-elemwise-datatype [r] :int64)
  dt-proto/PSubBuffer
  (-sub-buffer [r off len]
    (let [n-start (nth r off)
          n-end (nth r (+ off len))]
      (range n-start n-end (aget r "step"))))
  dt-proto/PSubBufferCopy
  (-sub-buffer-copy [r off len]
    (dt-proto/-sub-buffer r off len)))


(defn indexed-buffer
  "Given indexes and a buffer, return a new buffer that is ordered by the given indexes"
  [indexes buf]
  (if (and (dt-base/integer-range? indexes)
           (== 1 (aget indexes "step")))
    (let [n-elems (count buf)
          rstart (aget indexes "start")
          rend (aget indexes "end")]
      (if (and (== rstart 0)
               (== rend n-elems))
        buf
        (dt-base/sub-buffer buf
                            (aget indexes "start")
                            (- (aget indexes "end") (aget indexes "start")))))
    (let [buf (dt-base/ensure-indexable buf)
          dtype (dt-proto/-elemwise-datatype buf)
          indexes (dt-base/ensure-indexable indexes)
          n-indexes (count indexes)
          retval (make-array dtype n-indexes)
          ;;this code is structured to very carefully to take into account that boolean
          ;;arrays store their data as integer buffers.  Because the storage is different
          ;;than the presentation, those datatypes are not 'agetable' but because we
          ;;are just copying/reindexing data it is OK to use aget/aset.
          dest-buf (or (dt-base/as-js-array retval) (dt-base/as-typed-array retval))]
      (if-let [indexes (dt-base/as-agetable indexes)]
        (if-let [buf (or (dt-base/as-js-array buf) (dt-base/as-typed-array buf))]
          ;;buf is agetable
          (dotimes [idx n-indexes]
            (aset dest-buf idx (aget buf (aget indexes idx))))
          ;;buf is not agetable
          (dotimes [idx n-indexes]
            (aset dest-buf idx (nth buf (aget indexes idx)))))
        (if-let [buf (or (dt-base/as-js-array buf) (dt-base/as-typed-array buf))]
          (dotimes [idx n-indexes]
            (aset dest-buf idx (aget buf (nth indexes idx))))
          (dotimes [idx n-indexes]
            (aset dest-buf idx (nth buf (nth indexes idx))))))
      retval)))
