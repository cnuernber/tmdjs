(ns tech.v3.datatype.reader-vec
  (:require [tech.v3.datatype.copy-make-container :as dt-cmc]
            [tech.v3.datatype.arrays :as dt-arrays]
            [tech.v3.datatype.base :as dt-base]
            [tech.v3.datatype.protocols :as dt-proto]
            [tech.v3.datatype.casting :as casting]
            [clojure.string :as s]))


(declare reader-vec)


(deftype ReaderVec [meta idx->val cnt dtype ^:mutable __hash]
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
      (reader-vec cnt dtype idx->val meta)))

  IMeta
  (-meta [coll] meta)

  IStack
  (-peek [coll]
    (when (> cnt 0)
      (-nth coll (dec cnt))))
  (-pop [coll]
    (cond
     (zero? cnt) (throw (js/Error. "Can't pop empty vector"))
     (== 1 cnt) (-with-meta (.-EMPTY PersistentVector) meta)
     :else
     (reader-vec (dec cnt) dtype idx->val meta)))

  ICollection
  (-conj [coll o]
    (persistent! (conj! (transient coll) o)))

  IEmptyableCollection
  (-empty [coll] (-with-meta (.-EMPTY PersistentVector) meta))

  ISequential
  IEquiv
  (-equiv [coll other]
    (dt-arrays/equiv-nthable coll other))

  IHash
  (-hash [coll] (caching-hash coll hash-ordered-coll __hash))

  ISeqable
  (-seq [coll]
    (when-not (zero? cnt)
      (map #(nth coll %) (range cnt))))

  ICounted
  (-count [coll] cnt)

  IIndexed
  (-nth [coll n]
    (let [n (if (< n 0) (+ n cnt) n)]
      (when-not (< n cnt) (throw (js/Error. (str "nth out of range:" n " >= " cnt))))
      (idx->val n)))
  (-nth [coll n not-found]
    (let [n (if (< n 0) (+ n cnt) n)]
      (idx->val n)
      not-found))

  ILookup
  (-lookup [coll k] (-lookup coll k nil))
  (-lookup [coll k not-found] (if (number? k)
                                (-nth coll k not-found)
                                not-found))

  IAssociative
  (-assoc [coll k v]
    (if (number? k)
      (-assoc-n coll k v)
      (throw (js/Error. "Vector's key for assoc must be a number."))))
  (-contains-key? [coll k]
    (if (integer? k)
      (let [k (if (< k 0) (+ cnt k) k)]
        (and (<= 0 k) (< k cnt)))
      false))

  IFind
  (-find [coll n]
    (let [n (if (< n 0) (+ cnt n) n)]
      (when (and (<= 0 n) (< n cnt))
        (MapEntry. n (idx->val n) nil))))


  IVector
  (-assoc-n [coll n val]
    (let [n (if (< n 0) (+ cnt n) n)]
      (cond
        (and (<= 0 n) (< n cnt))
        (loop [idx 0
               nvec (transient [])]
          (if (< idx cnt)
            (recur (inc idx)
                   (if (== idx n)
                     (conj! nvec val)
                     (conj! nvec (idx->val n))))
            (persistent! nvec)))
        (== n cnt) (-conj coll val)
        :else (throw (js/Error. (str "Index " n " out of bounds  [0," cnt "]"))))))

  IReduce
  (-reduce [v f]
    (case cnt
      0 (f)
      1 (f (idx->val 0))
      (loop [idx 1
             init (f (idx->val 0))]
        (if (and (< idx cnt) (not (reduced? init)))
          (recur (inc idx) (f init (idx->val idx)))
          init))))
  (-reduce [v f init]
    (if (reduced? init)
      init
      (case cnt
        0 init
        1 (f init (idx->val 0))
        (loop [i 0
               init init]
          (if (and (< i cnt) (not (reduced? init)))
            (recur (inc i) (f init (idx->val i)))
            init)))))

  IKVReduce
  (-kv-reduce [v f init]
    (if (reduced? init)
      init
      (case cnt
        0 init
        1 (f init 0 (idx->val 0))
        (loop [idx 0
               init init]
          (if (and (< idx cnt) (not (reduced? init)))
            (recur (inc idx) (f init idx (idx->val idx))))))))

  IFn
  (-invoke [coll k]
    (-nth coll k))
  (-invoke [coll k not-found]
    (-nth coll k not-found))

  IEditableCollection
  (-as-transient [coll]
    (loop [idx 0
           retval (transient [])]
      (if (< idx cnt)
        (recur (inc idx) (conj! retval (idx->val idx)))
        retval)))

  IReversible
  (-rseq [coll]
    (when (pos? cnt)
      (map #(nth coll %) (range (dec cnt) -1 -1))))

  IIterable
  (-iterator [this]
    (dt-arrays/nth-iter this))

  IPrintWithWriter
  (-pr-writer [rdr writer opts]
    (-write writer (dt-base/reader->str rdr "reader")))

  dt-proto/PDatatype
  (-datatype [this] :reader)
  dt-proto/PElemwiseDatatype
  (-elemwise-datatype [this] dtype)
  dt-proto/PSubBufferCopy
  (-sub-buffer-copy [this off len]
    (dt-proto/-sub-buffer this off len))
  dt-proto/PSubBuffer
  (-sub-buffer [this off len]
    (let [off (if (< off 0) (+ cnt off) off)
          mlen (+ off len)]
      (when-not (<= mlen cnt)
        (throw (js/Error. (str "Off+len is out of range: " mlen " > " cnt))))
      (reader-vec len dtype #(idx->val (+ off %)) meta))))


(defn reader-vec
  ([n-elems dtype idx->val meta]
   (ReaderVec. meta idx->val n-elems dtype nil))
  ([n-elems dtype idx->val]
   (reader-vec n-elems dtype idx->val meta)))
