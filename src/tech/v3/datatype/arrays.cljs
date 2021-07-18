(ns tech.v3.datatype.arrays
  (:require [accent.arrays :as accent-arrays]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.base :as dt-base]
            [tech.v3.datatype.argtypes :as argtypes])
  (:refer-clojure :exclude [make-array]))


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


(doseq [ary-type (map first ary-types)]
  (extend-type ary-type
    dtype-proto/PElemwiseDatatype
    (-elemwise-datatype [item] (ary-types ary-type))
    dtype-proto/PDatatype
    (-datatype [item] :typed-array)
    dtype-proto/PToTypedArray
    (-convertible-to-typed-array? [item] true)
    (->typed-array [item] item)
    dtype-proto/PSubBufferCopy
    (-sub-buffer-copy [item off len]
      (.slice item off (+ off len)))
    dtype-proto/PSubBuffer
    (-sub-buffer [item off len]
      (.subarray item off (+ off len)))
    ICloneable
    (-clone [item]
      (let [len (aget item "length")
            retval (js* "new item.constructor(len)")]
        (.set retval item)
        retval))
    dtype-proto/PSetValue
    (-set-value! [item idx data]
      (cond
        (number? data)
        (aset item idx data)
        (accent-arrays/typed-array? data)
        (.set item data idx)
        (dtype-proto/-convertible-to-js-array? data)
        (dotimes [didx (count data)]
          (aset item (+ idx didx) (aget data didx)))
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
    dtype-proto/PSetConstant
    (-set-constant! [item offset elem-count data]
      (.fill item data offset (+ offset elem-count)))))



(extend-type array
  dtype-proto/PDatatype
  (-datatype [item] :js-array)
  dtype-proto/PToJSArray
  (-convertible-to-js-array? [buf] true)
  (->js-array [buf] buf)
  dtype-proto/PSubBufferCopy
  (-sub-buffer-copy [item off len]
    (.slice item off (+ off len)))
  dtype-proto/PSubBuffer
  (-sub-buffer [item off len]
    ;;js arrays, like jvm arrays, have no sub-array functionality
    (.slice item off (+ off len)))
  ICloneable
  (-clone [item] (.slice item 0 (count item)))
  dtype-proto/PSetValue
  (-set-value! [item idx data]
    (cond
      (number? data)
      (aset item idx data)
      (or (accent-arrays/typed-array? data)
          (dtype-proto/-convertible-to-js-array? data))
      (dotimes [didx (count data)]
        (aset item (+ idx didx) (aget data didx)))
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
  dtype-proto/PSetConstant
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
    (dtype-proto/-convertible-to-typed-array? data)
    (.map (dtype-proto/->typed-array data) bool-val->byte)
    (dtype-proto/-convertible-to-js-array? data)
    (.map (dtype-proto/->js-array data) bool-val->byte)
    (sequential? data)
    (mapv bool-val->byte data)
    ;;scalars should fall through here.
    :else
    (if data 1 0)))


(defn byte->boolean
  [val]
  (if (== 0 val) false true))

;;Booleans are stored as 1,0 bytes.
(deftype BooleanArray [buf metadata]
  ICounted
  (-count [item] (count buf))
  ICloneable
  (-clone [item] (BooleanArray. (clone buf) metadata))
  dtype-proto/PElemwiseDatatype
  (-elemwise-datatype [item] :boolean)
  dtype-proto/PSubBufferCopy
  (-sub-buffer-copy [item off len]
    (BooleanArray. (dtype-proto/-sub-buffer-copy buf off len) metadata))
  dtype-proto/PSubBuffer
  (-sub-buffer [item off len]
    (BooleanArray. (dtype-proto/-sub-buffer buf off len) metadata))
  dtype-proto/PSetValue
  (-set-value! [item idx data]
    (dtype-proto/-set-value! buf idx (booleans->bytes data))
    item)
  dtype-proto/PSetConstant
  (-set-constant! [item offset elem-count data]
    (dtype-proto/-set-constant! buf offset elem-count
                                (booleans->bytes data))
    item)
  dtype-proto/PToTypedArray
  (-convertible-to-typed-array? [this] true)
  (->typed-array [this] buf)
  ;;Disable aget for this buffer.  This is because it will result in algorithms
  ;;getting the base buffer which will mean they get 1,0 instead of true,false.
  dtype-proto/PAgetable
  (-convertible-to-agetable? [this] false)
  IWithMeta
  (-with-meta [coll new-meta]
    (if (identical? new-meta metadata)
      coll
      (BooleanArray. buf new-meta)))
  IMeta
  (-meta [coll] metadata)
  IPrintWithWriter
  (-pr-writer [array writer opts]
    (-write writer (str "#boolean-array"
                        (take 20 (map byte->boolean (array-seq buf))))))
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
  [buf]
  (BooleanArray. buf nil))


(declare make-typed-buffer)


;;Necessary to add an actual datatype to a js array and metadata to typed arrays
(deftype TypedBuffer [buf elem-dtype metadata]
  ICounted
  (-count [item] (count buf))
  ICloneable
  (-clone [item] (make-typed-buffer (clone buf) elem-dtype metadata))
  dtype-proto/PElemwiseDatatype
  (-elemwise-datatype [item] elem-dtype)
  dtype-proto/PToJSArray
  (-convertible-to-js-array? [item] (dtype-proto/-convertible-to-js-array? buf))
  (->js-array [item] (dtype-proto/->js-array buf))
  dtype-proto/PToTypedArray
  (-convertible-to-typed-array? [item] (dtype-proto/-convertible-to-typed-array? buf))
  (->typed-array [item] (dtype-proto/->typed-array buf))
  dtype-proto/PSubBufferCopy
  (-sub-buffer-copy [item off len]
    (make-typed-buffer (dt-base/sub-buffer-copy buf off len) elem-dtype metadata))
  dtype-proto/PSubBuffer
  (-sub-buffer [item off len]
    (make-typed-buffer (dt-base/sub-buffer buf off len) elem-dtype metadata))
  dtype-proto/PSetValue
  (-set-value! [item idx data]
    (dtype-proto/-set-value! buf idx data)
    item)
  dtype-proto/PSetConstant
  (-set-constant! [item offset elem-count data]
    (dtype-proto/-set-constant! buf offset elem-count data)
    item)
  IWithMeta
  (-with-meta [coll new-meta]
    (if (identical? new-meta metadata)
      coll
      (make-typed-buffer buf elem-dtype new-meta)))
  IMeta
  (-meta [coll] metadata)
  IPrintWithWriter
  (-pr-writer [array writer opts]
    (-write writer (str "#typed-buffer[" elem-dtype "]"
                        (take 20 (array-seq buf)))))
  ISequential
  ISeqable
  (-seq [array] (array-seq buf))
  ISeq
  (-first [array] (nth buf 0))
  (-rest  [array] (dtype-proto/-sub-buffer buf 1 (dec (count buf))))
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
        not-found))))


(defn make-typed-buffer
  [buf & [dtype metadata]]
  (let [dtype (or dtype (dt-base/elemwise-datatype buf))]
    (TypedBuffer. buf dtype metadata)))

;;Shorthand as this is very common
(defn tbuf [item] (make-typed-buffer item))


(defn make-array
  [dtype len]
  (if (= dtype :boolean)
    (BooleanArray. (js/Int8Array.  len) nil)
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
  dtype-proto/PElemwiseDatatype
  (-elemwise-datatype [r] :int64)
  dtype-proto/PSubBuffer
  (-sub-buffer [r off len]
    (let [n-start (nth r off)
          n-end (nth r (+ off len))]
      (range n-start n-end (aget r "step"))))
  dtype-proto/PSubBufferCopy
  (-sub-buffer-copy [r off len]
    (dtype-proto/-sub-buffer r off len)))


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
          dtype (dtype-proto/-elemwise-datatype buf)
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
