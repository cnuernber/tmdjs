(ns tech.v3.dataset.impl.column
  "Column implementation and defaults"
  (:require [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.argtypes :as argtypes]
            [tech.v3.datatype.arrays :as dt-arrays]
            [tech.v3.datatype.protocols :as dt-proto]
            [tech.v3.dataset.format-sequence :as fmt]
            [tech.v3.dataset.string-table :as strt]
            [tech.v3.dataset.protocols :as ds-proto]))


(defn datatype->missing-value
  [dtype]
  ;;these need to match the jvm values
  (case dtype
    :boolean false
    :int8 -128
    :int16 -32768
    :int32 -2147483648
    ;;really not enough bits for this but whatevs
    :int64 -9223372036854775808
    :float32 ##NaN
    :float64 ##NaN
    (if (casting/numeric-type? dtype)
      0
      nil)))


(defn make-container
  [dtype]
  (if (= :string dtype)
    (strt/make-string-table)
    (dtype/make-list dtype)))

(defn- inclusive-in-range
  [val start end]
  (and (>= val start)
       (<= val end)))

(defn- clamp
  [val start end]
  (max start (min val end)))

(defn- clip-integer-range
  [n-rows rng]
  (let [rstart (aget rng "start")
        rend (aget rng "end")]
    (if (and (inclusive-in-range rstart 0 (unchecked-dec n-rows))
             (inclusive-in-range rend -1 (unchecked-inc n-rows)))
      rng
      (let [rstart (clamp rstart 0 (unchecked-dec n-rows))
            rend (clamp rend -1 n-rows)]
        (range rstart rend (aget rng "step"))))))


(declare new-column)


(deftype Column [buf missing metadata numeric? ^:mutable hashcode]
  ICounted
  (-count [this] (count buf))
  ICloneable
  (-clone [this] (new-column (clone  buf) (clone missing) metadata numeric?))
  IFn
  (-invoke [this n] (nth this n))
  IIndexed
  (-nth [this n]
    (let [n (if (< n 0) (+ (count buf) n) n)]
      (if (.has missing n)
        (if numeric? ##NaN nil)
        (nth buf n))))
  (-nth [this n not-found]
    (let [n-elems (count buf)
          n (if (< n 0) (+ n-elems n) n)]
      (if (or (>= n n-elems) (.has missing n))
        not-found
        (nth buf n))))
  ISeqable
  (-seq [this]
    (map #(nth this %) (range (count this))))
  IWithMeta
  (-with-meta [coll new-meta]
    (if (identical? new-meta metadata)
      coll
      (new-column buf missing new-meta numeric?)))
  IMeta
  (-meta [coll]
    (assoc metadata
           :row-count (count buf)
           :datatype (dtype/elemwise-datatype buf)))
  IPrintWithWriter
  (-pr-writer [array writer opts]
    (let [dbuf (take 20 (seq array))
          dbuf (if numeric?
                 (fmt/format-sequence dbuf)
                 dbuf)
          dbuf (if (> (count array) 20)
                 (concat dbuf ["..."])
                 dbuf)
          dstr (.join (clj->js dbuf) " ")]
      (-write writer (str "#column[["
                          (name (dt-proto/-elemwise-datatype array))
                          ":" (count array)"]"
                          "[" dstr "]]"))))
  INamed
  (-name [this] (:name metadata))
  ISequential
  IHash
  (-hash [o]
    (when-not hashcode
      (set! hashcode (dt-arrays/hash-nthable o)))
    hashcode)
  IEquiv
  (-equiv [this other]
    (dt-arrays/equiv-nthable this other))
  IIterable
  (-iterator [this] (dt-arrays/nth-iter this))
  dt-proto/PElemwiseDatatype
  (-elemwise-datatype [this] (dtype/elemwise-datatype buf))
  dt-proto/PDatatype
  (-datatype [this] :column)
  dt-proto/PToJSArray
  (-convertible-to-js-array? [this] (and (dt-proto/-convertible-to-js-array? buf)
                                         (== 0 (aget missing "size"))))
  (->js-array [this] (dt-proto/->js-array buf))
  dt-proto/PToTypedArray
  (-convertible-to-typed-array? [this] (and (dt-proto/-convertible-to-typed-array? buf)
                                            (== 0 (aget missing "size"))))
  (->typed-array [this] (dt-proto/->typed-array buf))
  dt-proto/PAgetable
  (-convertible-to-agetable? [this] (and (dt-proto/-convertible-to-agetable? buf)
                                         (== 0 (aget missing "size"))))
  (->agetable [this] (dt-proto/->agetable buf))
  dt-proto/PSetValue
  (-set-value! [this idx data]
    (if (= :reader (argtypes/argtype data))
      (let [n-elems (count data)]
        (dotimes [elidx n-elems]
          (.remove missing (+ idx elidx))))
      (.remove missing idx))
    (dt-proto/-set-value! buf idx data))
  dt-proto/PSetConstant
  (-set-constant! [this idx elem-count value]
    (dotimes [elidx elem-count]
      (.remove missing (+ elidx idx)))
    (dt-proto/-set-constant! buf idx elem-count value))
  ds-proto/PColumn
  (-is-column? [this] true)
  ds-proto/PRowCount
  (-row-count [this] (count buf))
  ds-proto/PMissing
  (-missing [this] missing)
  ds-proto/PSelectRows
  (-select-rows [this rowidxs]
    (let [rowidxs (if (dtype/integer-range? rowidxs)
                    (clip-integer-range (count buf) rowidxs)
                    rowidxs)
          new-missing (js/Set.)]
      (when-not (== 0 (count missing))
        (dtype/indexed-iterate!
         #(when (.has missing %2)
            (.add new-missing %1))
         rowidxs))
      (new-column (dtype/indexed-buffer rowidxs buf) new-missing metadata numeric?))))


(defn new-column
  [buf missing metadata numeric?]
  (Column. buf missing metadata numeric? nil))
