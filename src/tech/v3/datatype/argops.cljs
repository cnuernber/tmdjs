(ns tech.v3.datatype.argops
  "Index-space algorithms.  Implements a subset of the jvm-version."
  (:require [tech.v3.datatype.base :as dt-base]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.copy-make-container :as dt-cmc]
            [tech.v3.datatype.list :as dt-list]
            [tech.v3.datatype.protocols :as dt-proto]))


(defn argsort
  "Return an array of indexes that order the provided data by compare-fn.  compare-fn must
  be a boolean function such as < or >.  You can use a full custom comparator returning
  -1,0 or 1 by using the `:comparator` option.

  * `compare-fn` - Boolean binary predicate such as < or >.

  Options:

  * `:nan-strategy` - defaults to `:last` - if the data has a numeric elemwise-datatype, a
  nan-aware comparsing will be used that will place nan data first, last, or throw an exception
  as specified by the three possible options - `:first`, `:last`, and `:exception`.
  * `:comparator` - comparator to use.  This overrides compare-fn and takes two arguments
  but returns a number.


  Examples:

```clojure
cljs.user> ;;Persistent vectors do not indicate datatype so nan-aware comparison is disabled.
cljs.user> (argops/argsort [##NaN 1 2 3 ##NaN])
#typed-buffer[[:int32 5][0 1 2 3 4]
cljs.user> ;;But with a container that indicates datatype nan will be handled
cljs.user> (argops/argsort (dtype/make-container :float32 [##NaN 1 2 3 ##NaN]))
 #typed-buffer[[:int32 5][1 2 3 4 0]
cljs.user> ;;example setting nan strategy and using custom comparator.
cljs.user> (argops/argsort nil  ;;no compare fn
                           {:nan-strategy :first
                            :comparator #(compare %2 %1)}
                           (dtype/make-container :float32 [##NaN 1 2 3 ##NaN]))
#typed-buffer[[:int32 5][0 4 3 2 1]
```"
  ([compare-fn options data]
   (let [comp (cond
                compare-fn
                (comparator compare-fn)
                (:comparator options)
                (:comparator options)
                :else
                compare)
         data (dt-base/ensure-indexable data)
         n-data (count data)
         indexes (dt-cmc/make-container :int32 (range n-data))
         idx-ary (dt-base/as-typed-array indexes)
         nan-strategy (get options :nan-strategy :last)]
     ;;agetable is a major optimization for sorting.  element access time means a lot
     ;;for a large nlogn op.
     (let [missing? (if (casting/numeric-type? (dt-base/elemwise-datatype data))
                      js/isNaN
                      nil?)
           [data get-fn] (if-let [aget-data (dt-base/as-agetable data)]
                           [aget-data aget]
                           [data nth])
           sort-fn (fn [lhs-idx rhs-idx]
                     (let [lhs (get-fn data lhs-idx)
                           rhs (get-fn data rhs-idx)
                           lhs-nan? (missing? lhs)
                           rhs-nan? (missing? rhs)]
                       (if (or lhs-nan? rhs-nan?)
                         (condp = nan-strategy
                           :exception
                           (throw (js/Error "NaN detected"))
                           :last  (if lhs-nan? 1 -1)
                           :first (if lhs-nan? -1 1))
                         (comp lhs rhs))))]
       (.sort idx-ary sort-fn))
     indexes))
  ([compare-fn data]
   (argsort compare-fn nil data))
  ([data]
   (argsort nil nil data)))


(defn numeric-truthy
  [val]
  (if (number? val)
    (and (not (js/isNaN val))
         (not= 0 val))
    val))


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
             (when (numeric-truthy (pred (aget data idx)))
               (dt-proto/-add indexes idx)))
           (dotimes [idx n-data]
             (when (numeric-truthy (pred (-nth data idx)))
               (dt-proto/-add indexes idx))))
         indexes))))
  ;;In this case the data itself must be truthy.
  ;;Avoids the use of an unnecessary predicate fn
  ([data]
   (let [data (dt-base/ensure-indexable data)
         n-data (count data)
         indexes (dt-list/make-list :int32)]
     (dotimes [idx n-data]
       (when (numeric-truthy (-nth data idx))
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
         data (dt-base/ensure-indexable data)
         n-elems (count data)]
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
               (recur (unchecked-dec low)))))))))
  ([data target]
   (binary-search data target nil)))
