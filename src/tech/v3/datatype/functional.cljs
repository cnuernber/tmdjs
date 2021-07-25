(ns tech.v3.datatype.functional
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


(defn sum [v] (stats/sum v))

(defn mean [v] (stats/mean v))

(defn variance [v] (stats/variance v))

(defn standard-deviation [v] (stats/standard-deviation v))

(defn percentiles
  "Percentiles are given in whole numbers:

```clojure
tech.v3.datatype.functional> (percentiles [0 25 50 75 100] (range 10))
[0.0 1.75 4.5 7.25 9.0]
```"
  ([percentages options v] (stats/percentiles percentages options v))
  ([percentages v] (stats/percentiles percentages v)))


(defn median
  ([options v] (stats/median options v))
  ([v] (stats/median v)))


(defn quartiles
  "return [min, 25 50 75 max] of item"
  ([item]
   (stats/quartiles item))
  ([options item]
   (stats/quartiles options item)))


(defn descriptive-statistics
  [stats v]
  (stats/descriptive-statistics stats v))


(defn magnitude-squared
  [v]
  (->> (dtype/emap #(* % %) :float64 v)
       (sum)))


(defn magnitude
  [v]
  (Math/sqrt (magnitude-squared v)))


(defn distance-squared
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
  [x y]
  (Math/sqrt (distance-squared x y)))


(defn equals
  [lhs rhs & [error-bar]]
  (cljs.core/< (double (distance lhs rhs))
               (double (cljs.core/or error-bar 0.001))))
