(ns tech.v3.datatype.base
  (:require [tech.v3.datatype.protocols :as dtype-proto])
  (:refer-clojure :exclude [clone counted?]))


(defn ecount
  [item]
  (if item
    ;;As count is a protocol in js, we have no reason to define our own
    (count item)
    0))


(defn clone
  "Here for compat with jvm system"
  [item]
  (cljs.core/clone item))


(defn elemwise-datatype
  [item]
  (if item
    (dtype-proto/-elemwise-datatype item)
    :object))


(defn datatype
  [item]
  (if item
    (dtype-proto/-datatype item)
    :object))


(defn as-typed-array
  [item]
  (when (and item (dtype-proto/-convertible-to-typed-array? item))
    (dtype-proto/->typed-array item)))


(defn as-js-array
  [item]
  (when (and item (dtype-proto/-convertible-to-js-array? item))
    (dtype-proto/->js-array item)))


(defn sub-buffer-copy
  "Create a copy of the data in the item from offset till len."
  [item off & [len]]
  (let [elen (count item)
        maxlen (- elen off)
        len (or len maxlen)]
    (when-not (>= elen (+ off len))
      (throw (js/Error. (str "Offset " off " len " len " => item length " elen))))
    (dtype-proto/-sub-buffer-copy item off len)))


(defn sub-buffer
  [item off & [len]]
  (let [elen (count item)
        maxlen (- elen off)
        len (or len maxlen)]
    (when-not (>= elen (+ off len))
      (throw (js/Error. (str "Offset " off " len " len " => item length " elen))))
    (dtype-proto/-sub-buffer item off len)))


(defn counted?
  [item]
  (when item
    (if (instance? js/Array item)
      true
      (cljs.core/counted? item))))


(defn ensure-indexable
  [data]
  (if-not (or (instance? js/Array data)
              (indexed? data))
    (vec data)
    data))


(defn as-agetable
  [data]
  (when (dtype-proto/-convertible-to-agetable? data)
    (dtype-proto/->agetable data)))


(defn set-value!
  [item idx data]
  (when-not item
    (throw (js/Error. "Item is nil")))
  (when-not (< idx (count item))
    (throw (js/Error. "Index is out of item length")))
  (when (and (counted? data)
             (not (<= (+ idx (count data)) (count item))))
    (throw (js/Error. (str "data length + idx " (+ (count data) idx)
                           " is out of range of item length ") (count item))))
  (dtype-proto/-set-value! item idx data)
  item)


(defn set-constant!
  [item idx elem-count data]
  (when-not item
    (throw (js/Error. "Item is nil")))
  (when-not (<= (+ idx elem-count) (count item))
    (throw (js/Error. "Index is out of item length")))
  (when (and (counted? data)
             (not (<= (+ idx (count data)) (count item))))
    (throw (js/Error. (str "data length + idx " (+ (count data) idx)
                           " is out of range of item length ") (count item))))
  (dtype-proto/-set-constant! item idx elem-count data)
  item)


(defn integer-range?
  [item]
  (instance? IntegerRange item))


(defn iterate-range!
  [consume-fn range]
  (let [start (aget range "start")
        step (aget range "step")
        n-elems (count range)]
    (if (and (= 0 start) (= 1 step))
      (dotimes [idx n-elems]
        (consume-fn idx))
      (dotimes [idx n-elems]
        (consume-fn (+ start (* idx step)))))
    consume-fn))


(defn indexed-iterate-range!
  [consume-fn range]
  (let [start (aget range "start")
        step (aget range "step")
        n-elems (count range)]
    (if (and (= 0 start) (= 1 step))
      (dotimes [idx n-elems]
        (consume-fn idx idx))
      (dotimes [idx n-elems]
        (consume-fn idx (+ start (* idx step)))))
    consume-fn))


(defn as-iterable
  [data]
  (when (aget data "values") data))


(defn indexed-iterate!
  [consume-fn item]
  (if-let [ary (as-agetable item)]
    (let [n-elems (count ary)]
      (dotimes [idx n-elems]
        (consume-fn idx (aget ary idx))))
    (cond
      (integer-range? item)
      (indexed-iterate-range! consume-fn item)
      (as-iterable item)
      (let [vals (.values item)]
        (loop [data (.next vals)
               idx 0]
          (when-not (.-done data)
            (consume-fn idx (.-value data))
            (recur (.next vals) (unchecked-inc idx)))))
      :else
      (if-let [item (seq item)]
        (loop [val (first item)
               item (rest item)
               idx 0]
          (consume-fn idx val)
          (when item
            (recur (first item) (rest item) (unchecked-inc idx)))))))
  consume-fn)


(defn iterate!
  [consume-fn item]
  (if-let [ary (as-agetable item)]
    (let [n-elems (count ary)]
      (dotimes [idx n-elems]
        (consume-fn (aget ary idx))))
    (cond
      (integer-range? item)
      (iterate-range! consume-fn item)
      (as-iterable item)
      (let [vals (.values item)]
        (loop [data (.next vals)]
          (when-not (.-done data)
            (consume-fn (.-value data))
            (recur (.next vals)))))
      (and (counted? item) (indexed? item))
      (let [n-elems (count item)]
        (dotimes [idx n-elems]
          (consume-fn (nth item idx))))
      :else
      (doseq [val item]
        (consume-fn val))))
  consume-fn)
