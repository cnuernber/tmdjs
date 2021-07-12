(ns tech.v3.datatype.argops
  (:require [tech.v3.datatype.base :as dt-base]
            [tech.v3.datatype.copy-make-container :as dt-cmc]))


(defn argsort
  ([compare-fn data]
   (let [comp (if compare-fn
                (comparator compare-fn)
                compare)
         data (dt-base/ensure-indexable data)
         n-data (count data)
         indexes (dt-cmc/make-container :int32 (range n-data))
         idx-ary (dt-base/as-typed-array indexes)]
     (if-let [data (dt-base/as-agetable data)]
       (.sort idx-ary #(comp (aget data %1) (aget data %2)))
       (.sort idx-ary #(comp (nth data %1) (nth data %2))))
     indexes))
  ([data]
   (argsort nil data)))


(defn argfilter
  [pred data]
  (let [data (dt-base/ensure-indexable data)
        n-data (count data)
        indexes (dt-cmc/make-container :int32 (range n-data))
        idx-ary (dt-base/as-typed-array indexes)]
    (if-let [data (dt-base/as-agetable data)]
      (.filter idx-ary #(boolean (pred (aget data %))))
      (.filter idx-ary #(boolean (pred (nth data %)))))))
