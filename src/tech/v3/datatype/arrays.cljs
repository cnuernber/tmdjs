(ns tech.v3.datatype.arrays
  (:require [accent.arrays :as accent-arrays]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.base :as dt-base])
  (:refer-clojure :exclude [make-array]))


(declare make-sub-buffer)


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
        (.set item idx data)
        (dtype-proto/-convertible-to-js-array? data)
        (dotimes [didx (count data)]
          (aset item (+ idx didx) (aget data didx)))
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
    (.subarray item off (+ off len)))
  ICloneable
  (-clone [item]
    (let [len (aget item "length")
          retval (js* "new item.constructor(len)")]
      (dotimes [idx len]
        (aset retval idx (aget item idx)))
      retval))
  dtype-proto/PSetValue
  (-set-value! [item idx data]
    (cond
      (number? data)
      (aset item idx data)
      (or (accent-arrays/typed-array? data)
          (dtype-proto/-convertible-to-js-array? data))
      (dotimes [didx (count data)]
        (aset item (+ idx didx) (aget data didx)))
      :else
      (dotimes [didx (count data)]
        (aset item (+ idx didx) (nth data didx))))
    item)
  dtype-proto/PSetConstant
  (-set-constant! [item offset elem-count data]
    (.fill item data offset (+ offset elem-count))))


(defn make-array
  [dtype len]
  (case dtype
    :int8 (js/Int8Array. len)
    :uint8 (js/Uint8Array. len)
    :int16 (js/Int16Array. len)
    :uint16 (js/Uint16Array. len)
    :int32 (js/Int32Array. len)
    :uint32 (js/Uint32Array. len)
    :float32 (js/Float32Array. len)
    :float64 (js/Float64Array. len)
    (js/Array. len)))


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
    (make-typed-buffer (dtype-proto/-sub-buffer-copy item off len) elem-dtype metadata))
  dtype-proto/PSubBuffer
  (-sub-buffer [item off len]
    (make-typed-buffer (dtype-proto/-sub-buffer item off len) elem-dtype metadata))
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
  (-rest  [array] (dtype-proto/-sub-buffer array 1 (dec (count array))))
  IFn
  (-invoke [array idx] (nth buf idx))
  IIndexed
  (-nth [array n] (nth buf n))
  (-nth [array n not-found]
    (if (< n (count buf))
      (nth buf n)
      not-found)))


(defn make-typed-buffer
  [buf dtype & [metadata]]
  (TypedBuffer. buf dtype metadata))


(extend-type IntegerRange
  dtype-proto/PElemwiseDatatype
  (-elemwise-datatype [r] :int64))
