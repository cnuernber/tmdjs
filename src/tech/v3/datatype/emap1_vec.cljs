(ns tech.v3.datatype.emap1-vec
  (:require [tech.v3.datatype.copy-make-container :as dt-cmc]
            [tech.v3.datatype.arrays :as dt-arrays]
            [tech.v3.datatype.base :as dt-base]
            [tech.v3.datatype.protocols :as dt-proto]
            [ham-fisted.api :as hamf]
            [ham-fisted.lazy-noncaching :as lznc]))


(declare emap1-vec)


(deftype Emap1Vec [meta map-fn dtype src ^:mutable __hash]
  Object
  (toString [coll]
    (pr-str* coll))
  (equiv [this other]
    (-equiv this other))
  (indexOf [coll x]
    (-indexOf coll x 0))
  (indexOf [coll x start]
    (-indexOf coll x start))
  (lastIndexOf [coll x]
    (-lastIndexOf coll x (count coll)))
  (lastIndexOf [coll x start]
    (-lastIndexOf coll x start))

  ICloneable
  (-clone [this] (dt-cmc/make-container dtype this))

  IWithMeta
  (-with-meta [coll new-meta]
    (if (identical? new-meta meta)
      coll
      (Emap1Vec. meta map-fn dtype src new-meta)))

  IMeta
  (-meta [_coll] meta)

  ISequential
  IEquiv
  (-equiv [coll other]
    (dt-arrays/equiv-nthable coll other))

  IHash
  (-hash [coll] (caching-hash coll hash-ordered-coll __hash))

  ISeqable
  (-seq [coll]
    (seq (lznc/map map-fn src)))

  ICounted
  (-count [_coll] (count src))

  IIndexed
  (-nth [_coll n]
    (let [l (count src)
          n (if (< n 0) (+ n l) n)]
      (when-not (< n l) (throw (js/Error. (str "nth out of range:" n " >= " l))))
      (map-fn (-nth src n))))
  (-nth [_coll n not-found]
    (let [l (count src)
          n (if (< n 0) (+ n l) n)]
      (map-fn (-nth src n))
      not-found))

  IReduce
  (-reduce [this rfn] (-reduce src #(rfn %1 (map-fn %2))))
  (-reduce [this rfn acc] (-reduce src #(rfn %1 (map-fn %2)) acc))

  ILookup
  (-lookup [coll k] (-lookup coll k nil))
  (-lookup [coll k not-found] (if (number? k)
                                (-nth coll k not-found)
                                not-found))

  IFind
  (-find [_coll n]
    (let [length (count src)
          n (if (< n 0) (+ length n) n)]
      (when (and (<= 0 n) (< n length))
        (MapEntry. n (-nth _coll n) nil))))


  IKVReduce
  (-kv-reduce [_v f init]
    (-reduce #(f %1 %2 (map-fn %2))
             init
             src))

  IFn
  (-invoke [coll k]
    (-nth coll k))
  (-invoke [coll k not-found]
    (-nth coll k not-found))

  IReversible
  (-rseq [coll]
    (seq (lznc/map map-fn (-rseq src))))

  IIterable
  (-iterator [this]
    (dt-arrays/nth-iter this))

  IPrintWithWriter
  (-pr-writer [rdr writer _opts]
    (-write writer (dt-base/reader->str rdr "reader")))

  dt-proto/PDatatype
  (-datatype [_this] :reader)
  dt-proto/PElemwiseDatatype
  (-elemwise-datatype [_this] dtype)
  dt-proto/PSubBufferCopy
  (-sub-buffer-copy [this off len]
    (dt-proto/-sub-buffer this off len))
  dt-proto/PSubBuffer
  (-sub-buffer [_this off len]
    (emap1-vec dtype map-fn (dt-proto/-sub-buffer src off len) meta)))


(defn emap1-vec
  ([dtype map-fn data meta]
   (Emap1Vec. meta map-fn dtype data nil))
  ([dtype map-fn data]
   (emap1-vec dtype map-fn data nil)))
