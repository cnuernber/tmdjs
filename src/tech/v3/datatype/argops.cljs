(ns tech.v3.datatype.argops
  "Index-space algorithms.  Implements a subset of the jvm-version."
  (:require [tech.v3.datatype.base :as dt-base]
            [tech.v3.datatype.copy-make-container :as dt-cmc]
            [tech.v3.datatype.list :as dt-list]
            [tech.v3.datatype.protocols :as dt-proto]))


(defn argsort
  "Return an array of indexes that order the provided data by compare-fn"
  ([compare-fn data]
   (let [comp (if compare-fn
                (comparator compare-fn)
                compare)
         data (dt-base/ensure-indexable data)
         n-data (count data)
         indexes (dt-cmc/make-container :int32 (range n-data))
         idx-ary (dt-base/as-typed-array indexes)]
     ;;agetable is a major optimization for sorting.  element access time means a lot
     ;;for a large nlogn op.
     (if-let [data (dt-base/as-agetable data)]
       (.sort idx-ary #(comp (aget data %1) (aget data %2)))
       (.sort idx-ary #(comp (nth data %1) (nth data %2))))
     indexes))
  ([data]
   (argsort nil data)))


(defn argfilter
  "Return an array of indexes that pass the filter."
  ([pred data]
   (let [data (dt-base/ensure-indexable data)
         n-data (count data)]
     (case :list-filter
       :ary-filter
       (let [indexes (dt-cmc/make-container :int32 (range n-data))
             idx-ary (dt-base/as-typed-array indexes)]
         (if-let [data (dt-base/as-agetable data)]
           (.filter idx-ary #(boolean (pred (aget data %))))
           (.filter idx-ary #(boolean (pred (nth data %)))))
         indexes)
       :list-filter
       (let [indexes (dt-list/make-list :int32)
             n-data (count data)]
         (if-let [data (dt-base/as-agetable data)]
           (dotimes [idx n-data]
             (when (pred (aget data idx))
               (dt-proto/-add indexes idx)))
           (dotimes [idx n-data]
             (when (pred (-nth data idx))
               (dt-proto/-add indexes idx))))
         indexes))))
  ;;In this case the data itself must be truthy.
  ;;Avoids the use of an unnecessary predicate fn
  ([data]
   (let [data (dt-base/ensure-indexable data)
         n-data (count data)
         indexes (dt-list/make-list :int32)]
     (dotimes [idx n-data]
       (when (-nth data idx)
         (dt-proto/-add indexes idx)))
     indexes)))


(defn arggroup
  "Return a map from value->indexes that hold that value."
  [data]
  (let [data (dt-base/ensure-indexable data)
        n-elems (count data)]
    (if-let [data (dt-base/as-agetable data)]
      (loop [idx 0
             retval (transient {})]
        (if (< idx n-elems)
          (let [value (aget data idx)
                dlist (get retval value)
                has-dlist? (boolean dlist)
                dlist (if has-dlist? dlist (dt-list/make-list :int32))]
            (dt-proto/-add dlist idx)
            (recur (unchecked-inc idx) (if has-dlist?
                                         retval
                                         (assoc! retval value dlist))))
          (persistent! retval)))
      (loop [idx 0
             retval (transient {})]
        (if (< idx n-elems)
          (let [value (nth data idx)
                dlist (get retval value)
                has-dlist? (boolean dlist)
                dlist (if has-dlist? dlist (dt-list/make-list :int32))]
            (dt-proto/-add dlist idx)
            (recur (unchecked-inc idx) (if has-dlist?
                                         retval
                                         (assoc! retval value dlist))))
          (persistent! retval))))))

(defn arglast-every
  "Return the last index where (pred (rdr idx) (rdr (dec idx))) was true by
  comparing every value and keeping track of the last index where pred was true."
  [rdr pred]
  (let [rdr (dt-base/ensure-indexable rdr)
        n-elems (count rdr)]
    (if-let [rdr (dt-base/as-agetable rdr)]
      (loop [idx 1
             max-idx 0
             max-value (aget rdr 0)]
        (if (== n-elems idx)
          max-idx
          (let [cur-val (aget rdr idx)
                found? (pred cur-val max-value)]
            (recur (unchecked-inc idx)
                   (if found? idx max-idx)
                   (if found? cur-val max-value)))))
      (loop [idx 1
             max-idx 0
             max-value (nth rdr 0)]
        (if (== n-elems idx)
          max-idx
          (let [cur-val (nth rdr idx)
                found? (pred cur-val max-value)]
            (recur (unchecked-inc idx)
                   (if found? idx max-idx)
                   (if found? cur-val max-value))))))))


(defn argmax
  "Return the last index of the max item in the reader."
  [rdr]
  (arglast-every rdr >))


(defn argmin
  "Return the last index of the min item in the reader."
  [rdr]
  (arglast-every rdr <))


(defn binary-search
  "Returns a long result that points to just before the value or exactly points to the
   value.  In the case where the target is after the last value will return
  elem-count.  If value is present multiple times the index will point to the first
  value.

  Options:

  * `:comparator` - a specific comparator to use; defaults to `comparator`."
  ([data target options]
   (let [comp (get options :comparator compare)
         data (dt-base/ensure-indexable data)]
     (let [n-elems (count data)]
       (loop [low (long 0)
              high n-elems]
         (if (< low high)
           (let [mid (+ low (quot (- high low) 2))
                 buf-data (nth data mid)
                 compare-result (comp buf-data target)]
             (if (== 0 compare-result)
               (recur mid mid)
               (if (and (< compare-result 0)
                        (not= mid low))
                 (recur mid high)
                 (recur low mid))))
           (loop [low low]
             (let [buf-data (nth data low)
                   comp (comp target buf-data)]
               (cond
                 (or (< comp 0) (== 0 low)) low
                 (> comp 0) (unchecked-inc low)
                 ;;When values are equal, track backward to first non-equal member.
                 :else
                 (recur (unchecked-dec low))))))))))
  ([data target]
   (binary-search data target nil)))
