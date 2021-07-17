(ns tech.v3.datatype.list
  (:require [tech.v3.datatype.base :as dt-base]
            [tech.v3.datatype.copy-make-container :as dt-cmc]
            [tech.v3.datatype.protocols :as dt-proto]
            [tech.v3.datatype.arrays :as dt-arrays]))


(declare make-primitive-list)


(deftype PrimitiveList [^:unsynchronized-mutable buf
                        ^:unsynchronized-mutable agetable?
                        dtype
                        ^:unsynchronized-mutable ptr
                        ^:unsynchronized-mutable buflen
                        metadata]
  ICounted
  (-count [this] ptr)
  ICloneable
  (-clone [this] (make-primitive-list
                  (-> (dt-proto/-sub-buffer buf 0 ptr)
                      (clone))
                  dtype ptr))
  IMeta
  (-meta [this] metadata)
  IWithMeta
  (-with-meta [this new-meta]
    (make-primitive-list buf dtype ptr new-meta))
  IPrintWithWriter
  (-pr-writer [array writer opts]
    (-write writer (str "#list[" dtype "]"
                        (take 20 (seq (dt-proto/-sub-buffer buf 0 ptr))))))
  ISequential
  ISeqable
  (-seq [array] (array-seq (dt-proto/-sub-buffer buf 0 ptr)))
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
    (let [n-ary (count array)
          n (if (< n 0) (+ (count array) n) n)]
      (if (< n n-ary)
        (nth buf n)
        not-found)))
  dt-proto/PElemwiseDatatype
  (-elemwise-datatype [this] dtype)
  dt-proto/PDatatype
  (-datatype [this] :list)
  dt-proto/PSubBufferCopy
  (-sub-buffer-copy [item offset length]
    (dt-arrays/make-typed-buffer (dt-proto/-sub-buffer-copy buf offset length)
                                 dtype metadata))
  dt-proto/PSubBuffer
  (-sub-buffer [item offset length]
    (dt-arrays/make-typed-buffer (dt-proto/-sub-buffer buf offset length)
                                 dtype metadata))
  dt-proto/PToTypedArray
  (-convertible-to-typed-array? [this] (dt-proto/-convertible-to-typed-array? buf))
  (->typed-array [this] (dt-proto/->typed-array (dt-proto/-sub-buffer buf 0 ptr)))

  dt-proto/PToJSArray
  (-convertible-to-js-array? [this] (dt-proto/-convertible-to-js-array? buf))
  (->js-array [this] (dt-proto/->js-array (dt-proto/-sub-buffer buf 0 ptr)))


  dt-proto/PAgetable
  (-convertible-to-agetable? [this] (dt-proto/-convertible-to-agetable? buf))
  (->agetable [this] (dt-proto/->agetable (dt-proto/-sub-buffer buf 0 ptr)))

  dt-proto/PSetValue
  (-set-value! [item idx data]
    (-> (dt-proto/-sub-buffer buf 0 ptr)
        ;;use base version as it has error checking
        (dt-base/set-value! idx data))
    item)
  dt-proto/PSetConstant
  (-set-constant! [item offset elem-count data]
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
      (aset buf ptr elem)
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
      ;;For sequences we just add the data directly
      (doseq [data container]
        (dt-proto/-add this data)))
    this))


(defn make-primitive-list
  [buf & [dtype ptr metadata]]
  (let [dtype (or dtype (dt-proto/-elemwise-datatype buf))
        ptr (or ptr 0)
        buflen (count buf)
        abuf (dt-base/as-agetable buf)
        agetable? (if abuf true false)]
    (PrimitiveList. (or abuf buf) agetable? dtype ptr buflen metadata)))


(defn make-list
  [dtype & [initial-bufsize]]
  (let [initial-bufsize (or initial-bufsize 4)]
    (make-primitive-list (dt-arrays/make-array dtype initial-bufsize)
                         dtype 0)))