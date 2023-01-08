(ns tech.v3.datatype.statistics
  (:require [tech.v3.datatype :as dtype]
            [ham-fisted.api :as hamf])
  (:refer-clojure :exclude [min max]))


(deftype GenReducer [^:mutable total ^:mutable n-elems reduce-fn nan-behavior]
  IFn
  (-invoke [this arg]
    (let [arg (if (js/isNaN arg)
                (nan-behavior arg)
                arg)]
      (when-not (nil? arg)
        (set! total (if (== 0 n-elems)
                      arg
                      (reduce-fn total arg)))
        (set! n-elems (inc n-elems))))
    this)
  IPrintWithWriter
  (-pr-writer [this w _options]
    (-write w (str "#sum-reducer" @this)))
  IDeref
  (-deref [_this]
    {:sum total
     :n-elems n-elems}))


(defn ->nan-behavior
  [nan-strategy]
  (case (or nan-strategy :remove)
    :remove (constantly nil)
    :keep identity
    :exception #(throw (js/Error. "NaN detected in data"))))


(defn sum-reducer [& [nan-strategy]]
  (GenReducer. 0 0 + (->nan-behavior (or nan-strategy :remove))))


(defn min-reducer [& [nan-strategy]]
  (GenReducer. 0 0 cljs.core/min (->nan-behavior (or nan-strategy :remove))))


(defn max-reducer [& [nan-strategy]]
  (GenReducer. 0 0 cljs.core/max (->nan-behavior (or nan-strategy :remove))))


(defn sum
  [v]
  (hamf/sum v))


(defn min
  [v]
  (->> (hamf/apply-nan-strategy nil v)
       (hamf/mmin-key identity)))


(defn max
  [v]
  (->> (hamf/apply-nan-strategy nil v)
       (hamf/mmin-key identity)))


(defn mean
  [v]
  (hamf/mean v))


(defn variance
  [v]
  (hamf/variance v))


(defn standard-deviation
  [v]
  (hamf/standard-deviation v))


(defn mode
  [v]
  (hamf/mode v))


(defn- ensure-sorted
  "ensure v is sorted returning a typed buffer of data."
  [options v]
  (let [v (hamf/apply-nan-strategy options v)
        tbuf (or (dtype/as-typed-array v)
                 (-> (dtype/make-container :float64 v)
                     (dtype/as-typed-array)))]
    (.sort tbuf)
    tbuf))


(defn percentiles
  "Percentiles are given in whole numbers:

```clojure
tech.v3.datatype.functional> (percentiles [0 25 50 75 100] (range 10))
[0.0 1.75 4.5 7.25 9.0]
```"
  ([percentages options v]
   (let [v (ensure-sorted options v)
         nv (inc (count v))]
     (-> (->> percentages
              (dtype/emap
               (fn [percentage]
                 (let [percentage (/ percentage 100.0)]
                   (if (>= percentage 1.0)
                     (aget v (dec (count v)))
                     (let [rank (* nv percentage)]
                       (if (== rank (Math/floor rank))
                         (aget v rank)
                         (let [lrank (Math/floor rank)
                               leftover (- rank lrank)
                               lval (aget v (dec lrank))
                               diff (- (aget v lrank)
                                       lval)]
                           (+ lval (* diff leftover))))))))
               :float64)
              ;;realize the result
              (clone))
         (vary-meta assoc :simple-print? true))))
  ([percentages v]
   (percentiles percentages nil v)))


(defn median
  ([options v]
   (nth (percentiles [50] options v) 0))
  ([v] (median nil v)))


(defn quartiles
  "return [min, 25 50 75 max] of item"
  ([item]
   (percentiles [0 25 50 75 100] item))
  ([options item]
   (percentiles [0 25 50 75 100] options item)))


;;we can worry about a more efficient, fewer-pass pathway later.
(def ^:private stat-fn-map
  {:min min
   :max max
   :sum sum
   :mean mean
   :median median
   :variance variance
   :standard-deviation standard-deviation
   :n-values #(-> (dtype/iterate! (sum-reducer) %)
                  (deref)
                  :n-elems)})


(defn descriptive-statistics
  [stats v]
  (->> stats
       (map (fn [stat]
              (if-let [sfn (get stat-fn-map stat)]
                [stat (sfn v)]
                (throw (js/Error. (str "Unrecognized stat " stat))))))
       (into {})))
