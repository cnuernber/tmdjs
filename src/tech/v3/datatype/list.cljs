(ns tech.v3.datatype.list
  (:require [tech.v3.datatype.base :as dt-base]
            [tech.v3.datatype.copy-make-container :as dt-cmc]
            [tech.v3.datatype.protocols :as dt-proto]
            [tech.v3.datatype.arrays :as dt-arrays]
            [tech.v3.datatype.casting :as casting]))


(declare make-primitive-list)


(deftype PrimitiveList [^:unsynchronized-mutable buf
                        ^:unsynchronized-mutable agetable?
                        dtype
                        cast-fn
                        ^:unsynchronized-mutable ptr
                        ^:unsynchronized-mutable buflen
                        ^:unsynchronized-mutable hashcode
                        metadata]
  ICounted
  (-count [_this] ptr)
  ICloneable
  (-clone [_this] (make-primitive-list
                   (-> (dt-proto/-sub-buffer buf 0 ptr)
                       (clone))
                   dtype ptr))
  IMeta
  (-meta [_this] metadata)
  IWithMeta
  (-with-meta [_this new-meta]
    (make-primitive-list buf dtype ptr new-meta))
  IPrintWithWriter
  (-pr-writer [rdr writer _opts]
    (-write writer (dt-base/reader->str rdr "list")))
  ISequential
  ISeqable
  (-seq [_array] (array-seq (dt-proto/-sub-buffer buf 0 ptr)))
  ISeq
  (-first [_array] (nth buf 0))
  (-rest  [_array] (dt-proto/-sub-buffer buf 1 (dec (count buf))))
  IFn
  (-invoke [array n]
    (let [n (if (< n 0) (+ (count array) n) n)]
      (nth buf n)))
  IIndexed
  (-nth [array n]
    (dt-arrays/nth-impl n (count array) ::dt-arrays/exception
                        nth buf))
  (-nth [array n not-found]
    (dt-arrays/nth-impl n (count array) not-found
                        nth buf))
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
  (-elemwise-datatype [_this] dtype)
  dt-proto/PDatatype
  (-datatype [_this] :list)
  dt-proto/PSubBufferCopy
  (-sub-buffer-copy [_item offset length]
    (dt-arrays/make-typed-buffer (dt-proto/-sub-buffer-copy buf offset length)
                                 dtype metadata))
  dt-proto/PSubBuffer
  (-sub-buffer [_item offset length]
    (dt-arrays/make-typed-buffer (dt-proto/-sub-buffer buf offset length)
                                 dtype metadata))
  dt-proto/PToTypedArray
  (-convertible-to-typed-array? [_this] (dt-proto/-convertible-to-typed-array? buf))
  (->typed-array [_this] (dt-proto/->typed-array (dt-proto/-sub-buffer buf 0 ptr)))

  dt-proto/PToJSArray
  (-convertible-to-js-array? [_this] (dt-proto/-convertible-to-js-array? buf))
  (->js-array [_this] (dt-proto/->js-array (dt-proto/-sub-buffer buf 0 ptr)))


  dt-proto/PAgetable
  (-convertible-to-agetable? [_this] (dt-proto/-convertible-to-agetable? buf))
  (->agetable [_this] (dt-proto/->agetable (dt-proto/-sub-buffer buf 0 ptr)))

  dt-proto/PSetValue
  (-set-value! [item idx data]
    (-> (dt-proto/-sub-buffer buf 0 ptr)
        ;;use base version as it has error checking
        (dt-base/set-value! idx data))
    item)
  dt-proto/PSetConstant
  (-set-constant! [_item offset elem-count data]
    (-> (dt-proto/-sub-buffer buf 0 ptr)
        (dt-base/set-constant! offset elem-count data)))
  dt-proto/PListLike
  (-add [this elem]
    (when (== ptr buflen)
      (let [new-buf (dt-cmc/make-container dtype (* 2 buflen))
            abuf (dt-base/as-agetable new-buf)
            new-agetable? (boolean abuf)]
        (dt-base/set-value! new-buf 0 buf)
        (set! buf (or abuf new-buf))
        (set! agetable? new-agetable?)
        (set! buflen (* 2 buflen))))
    (if agetable?
      (aset buf ptr (cast-fn elem))
      (dt-proto/-set-value! buf ptr elem))
    (set! ptr (inc ptr))
    this)
  (-add-all [this container]
    (if (indexed? container)
      (let [n-elems (count container)]
        (when (> (+ ptr n-elems) buflen)
          (let [new-len (* 2 (+ ptr n-elems))
                new-buf (dt-cmc/make-container dtype new-len)
                abuf (dt-base/as-agetable new-buf)
                new-agetable? (boolean abuf)]
            (dt-base/set-value! new-buf 0 buf)
            (set! buf (or abuf new-buf))
            (set! agetable? new-agetable?)
            (set! buflen new-len)))
        (dt-base/set-value! buf ptr container)
        (set! ptr (+ ptr n-elems)))
      (dt-base/iterate! #(dt-proto/-add this %) container))
    this)
  (-ensure-capacity [this capacity]
    (when (< buflen capacity)
      (let [new-len buflen
            new-buf (dt-cmc/make-container dtype new-len)
            abuf (dt-base/as-agetable new-buf)
            new-agetable? (boolean abuf)]
        (dt-base/set-value! new-buf 0 buf)
        (set! buf (or abuf new-buf))
        (set! agetable? new-agetable?)
        (set! buflen new-len)))
    this))


(defn make-primitive-list
  [buf & [dtype ptr metadata]]
  (let [dtype (or dtype (dt-proto/-elemwise-datatype buf))
        ptr (or ptr 0)
        buflen (count buf)
        abuf (dt-base/as-agetable buf)
        agetable? (if abuf true false)]
    (PrimitiveList. (or abuf buf) agetable? dtype
                    (casting/cast-fn dtype)
                    ptr buflen metadata nil)))


(defn make-list
  [dtype & [initial-bufsize]]
  (let [initial-bufsize (or initial-bufsize 4)]
    (make-primitive-list (dt-arrays/make-array dtype initial-bufsize)
                         dtype 0)))
