(ns tech.v3.datatype.functional
  "Simple math primitives."
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.statistics :as stats]))


(defn reduce-min
  "Nan-unaware min.  tech.v3.datatype.statistics/min is nan-aware"
  [v]
  (reduce min v))


(defn reduce-max
  "Nan-unaware max.  tech.v3.datatype.statistics/max is nan-aware"
  [v]
  (reduce max v))


(defn sum
  "Nan-aware sum.  Nan's will be skipped."
  [v]
  (stats/sum v))

(defn mean
  "Nan-aware mean.  Nan's will be skipped."
  [v]
  (stats/mean v))

(defn variance
  "Nan-aware variance.  Nan's will be skipped."
  [v]
  (stats/variance v))

(defn standard-deviation
  "Nan-aware standard-deviation.  Nan's will be skipped."
  [v] (stats/standard-deviation v))

(defn percentiles
  "Percentiles are given in whole numbers:

```clojure
tech.v3.datatype.functional> (percentiles [0 25 50 75 100] (range 10))
[0.0 1.75 4.5 7.25 9.0]
```"
  ([percentages options v] (stats/percentiles percentages options v))
  ([percentages v] (stats/percentiles percentages v)))


(defn median
  "Nan-aware median.  Nan's will be skipped."
  ([options v] (stats/median options v))
  ([v] (stats/median v)))


(defn quartiles
  "return [min, 25 50 75 max] of item"
  ([item]
   (stats/quartiles item))
  ([options item]
   (stats/quartiles options item)))


(defn descriptive-statistics
  "Given a sequence of desired stats return a map of statname->value.

  Example:

```clojure
cljs.user> (dfn/descriptive-statistics [:min :max :mean :n-values] (range 10))
{:min 0, :max 9, :mean 4.5, :n-values 10}
```"
  [stats v]
  (stats/descriptive-statistics stats v))


(defn magnitude-squared
  "Magnitude-squared of the vector"
  [v]
  (->> (dtype/emap #(* % %) :float64 v)
       (sum)))


(defn magnitude
  "Magnitude of the vector"
  [v]
  (Math/sqrt (magnitude-squared v)))


(defn distance-squared
  "Euclidian distance squared between x,y"
  [x y]
  (when-not (== (count x) (count y))
    (throw (js/Error. "X,y have different lengths")))
  (let [xlen (count x)]
    (if (== 0 xlen)
      0
      (->> (dtype/emap #(let [v (- %1 %2)]
                          (* v v))
                       :float64
                       x y)
           (sum)))))


(defn distance
  "Euclidian distance between x,y"
  [x y]
  (Math/sqrt (distance-squared x y)))


(defn equals
  "Numeric equals - the distance between x,y must be less than error-bar which defaults
  to 0.001."
  [lhs rhs & [error-bar]]
  (cljs.core/< (double (distance lhs rhs))
               (double (cljs.core/or error-bar 0.001))))
