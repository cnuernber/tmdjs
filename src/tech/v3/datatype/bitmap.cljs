(ns tech.v3.datatype.bitmap
  "set implementation specialized towards unsigned 32 bit integers."
  (:require [tech.v3.datatype.protocols :as dt-proto]
            [tech.v3.datatype.base :as dt-base]
            [tech.v3.datatype.copy-make-container :as dt-cmc]
            [tech.v3.datatype.list :as dt-list]
            [clojure.set :as set]))


(extend-type js/Set
  ICounted
  (-count [s] (aget s "size")))


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
   (if (nil? data)
     (->js-set)
     (cond
       (instance? js/Set data)
       data
       (dt-base/as-agetable data)
       (let [data (dt-base/as-agetable data)
             n-data (count data)
             retval (js/Set.)]
         (dotimes [idx n-data]
           (.add retval (aget data idx)))
         retval)
       (dt-base/integer-range? data)
       (let [retval (js/Set!.)]
         (dt-base/iterate-range! #(.add retval %) data)
         retval)
       (as-iterable data)
       (let [data (as-iterable data)
             retval (js/Set.)]
         (dotimes [idx n-data]
           (.add retval (nth data idx)))
         retval)
       :else
       (let [retval (js/Set.)]
         (doseq [item data]
           (.add retval item))
         retval)))))


(defn ->bitmap
  "compat with jvm"
  ([] (->js-set))
  ([data] (->js-set data)))


(extend-type js/Set
  dt-proto/PBitmapSet
  (-set-or [lhs rhs]
    (let [rhs (->iterable rhs)
          retval (js/Set. lhs)]
      (iterate-set! #(.add retval %) rhs)
      retval))
  (-set-and [lhs rhs]
    (let [rhs (->iterable rhs)
          retval (js/Set.)]
      (iterate-set! #(when (.has lhs %)
                       (.add retval %))
                    rhs)
      retval))
  (-set-and-not [lhs rhs]
    )
  (-set-offset [lhs off]
    (let [retval (js/Set.)]
      (iterate-set! #(.add retval (+ % off)) lhs)
      retval)))


;;These work but are slower than their clojurescript implementations.
(extend-type PersistentHashSet
  dt-proto/PBitmapSet
  (-set-or [lhs rhs] (set/union lhs rhs))
  (-set-and [lhs rhs] (set/intersection lhs rhs))
  (-set-and-not [lhs rhs] (set/difference lhs rhs))
  (-set-offset [lhs off]
    (let [retval (js/Set.)]
      (iterate-set! #(.add retval (+ % off)) lhs)
      retval)))


(defn set->ordered-indexes
  "Return the value in the set in an int32 array ordered from least to greatest."
  [data]
  (let [indexes (dt-list/make-list :int32 (count data))
        buffer (dt-base/as-agetable indexes)]
    (iterate-set! #(dt-proto/-add indexes %) data)
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


(comment
  ;;construction
  (def ignored
    (do
      (println "set construction")
      (def data (time (->bitmap (range 1000000))))
      (println "set union")
      (time (dt-proto/-set-or data data))
      (println "to ordered indexes")
      (time (set->ordered-indexes data))))


  ;; set construction
  ;; "Elapsed time: 140.462971 msecs"
  ;; set union
  ;; "Elapsed time: 204.093573 msecs"
  ;; to ordered indexes
  ;; "Elapsed time: 0.272743 msecs"


  (def ignored
    (do
      (println "set construction")
      (def data (time (set (range 1000000))))
      (println "set-union")
      (time (set/union data data))
      (println "to ordered indexes")
      (time (let [indexes (dt-list/make-list :int32 (count data))
                  buffer (dt-base/as-agetable indexes)]
              (iterate-set! #(dt-proto/-add indexes %) data)
              (.sort buffer)
              indexes))))

;; set construction
;; "Elapsed time: 1135.150053 msecs"
;; set-union
;; "Elapsed time: 789.842971 msecs"
;; to ordered indexes
;; "Elapsed time: 0.179169 msecs"


  )
