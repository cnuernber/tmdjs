(ns tech.v3.datatype.bitmap
  "set implementation specialized towards unsigned 32 bit integers."
  (:require [tech.v3.datatype.protocols :as dt-proto]
            [tech.v3.datatype.base :as dt-base]
            [tech.v3.datatype.list :as dt-list]
            [clojure.set :as set]))


(extend-type js/Set
  ICounted
  (-count [s] (aget s "size"))
  ICloneable
  (-clone [s] (js/Set. s)))


(defn ->iterable
  "make something iterable"
  [data]
  (if (aget data "values")
    data
    (clj->js data)))


(defn as-iterable
  [data]
  (when (aget data "values") data))


(defn ->js-set
  ([] (js/Set.))
  ([data]
   (cond
     (nil? data)
     (->js-set)
     (instance? js/Set data)
     data
     :else
     (reduce #(do (.add ^JS %1 %2) %1)
             (js/Set.)
             data))))


(defn ->bitmap
  "compat with jvm"
  ([] (->js-set))
  ([data] (->js-set data)))


(extend-type js/Set
  dt-proto/PBitmapSet
  (-set-or [lhs rhs]
    (let [rhs (->iterable rhs)
          retval (js/Set. lhs)]
      (dt-base/iterate! #(.add retval %) rhs)
      retval))
  (-set-and [lhs rhs]
    (let [rhs (->iterable rhs)
          retval (js/Set.)]
      (dt-base/iterate! #(when (.has lhs %)
                           (.add retval %))
                        rhs)
      retval))
  (-set-and-not [lhs rhs]
    (let [retval (js/Set. lhs)]
      (dt-base/iterate! #(.delete retval %) rhs)
      retval))
  (-set-offset [lhs off]
    (let [retval (js/Set.)]
      (dt-base/iterate! #(.add retval (+ % off)) lhs)
      retval)))


;;These work but are slower than their clojurescript implementations.
(extend-type PersistentHashSet
  dt-proto/PBitmapSet
  (-set-or [lhs rhs] (set/union lhs rhs))
  (-set-and [lhs rhs] (set/intersection lhs rhs))
  (-set-and-not [lhs rhs] (set/difference lhs rhs))
  (-set-offset [lhs off]
    (let [retval (js/Set.)]
      (dt-base/iterate! #(.add retval (+ % off)) lhs)
      retval)))


(defn set->ordered-indexes
  "Return the value in the set in an int32 array ordered from least to greatest."
  [data]
  (let [indexes (dt-list/make-list :int32 (count data))
        buffer (dt-base/as-agetable indexes)]
    (dt-base/iterate! #(dt-proto/-add indexes %) data)
    (.sort buffer)
    indexes))


(defn bitmap->efficient-random-access-reader
  "old name for [[set->ordered-indexes]]"
  [bitmap]
  (set->ordered-indexes bitmap))


(defn js-set->clj
  [js-set]
  (let [values (.values js-set)]
    (loop [retval (transient (set nil))]
      (let [next-val (.next values)]
        (if (.-done next-val)
          (persistent! retval)
          (recur (conj! retval (.-value next-val))))))))
