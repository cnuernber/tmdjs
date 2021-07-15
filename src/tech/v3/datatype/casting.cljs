(ns tech.v3.datatype.casting
  (:require [clojure.set :as set]))

(def type-graph-data
  [[:boolean :int8 :uint8]
   [:int8 :int16]
   [:uint8 :int16 :uint16]
   [:int16 :int32 :float32]
   [:uint16 :int32 :uint32 :float32]
   [:int32 :float64]
   [:uint32 :float64]
   [:float32 :float64]
   [:float64 :object]
   ;;int64 doesn't exist in js land so these are auto-promoted to object
   ;;containers
   [:int64 :object]
   [:uint64 :object]])


(def datatype-vec [:boolean :int8 :uint8 :int16 :uint16 :int32 :uint32
                   :float32 :float64 :int64 :uint64 :object])

(def datatype-rank
  (->> datatype-vec
       (map-indexed vector)
       (map (comp vec reverse))
       (into {})))


(defn smallest-datatype
  [dt-seq]
  (reduce (fn [lhs rhs]
            (if (< (datatype-rank lhs)
                   (datatype-rank rhs))
              lhs
              rhs))
          dt-seq))


(def type-graph
  (reduce (fn [tg entry]
            (let [new-dt (first entry)
                  valid-types (set entry)
                  tg (update tg new-dt set/union valid-types)]
              (->> tg
                   (map (fn [[k v]]
                          [k (if (v new-dt)
                               (set/union v valid-types)
                               v)]))
                   (into {}))))
          {}
          type-graph-data))


(defn widest-datatype
  ([dtype] dtype)
  ([lhs-dtype rhs-dtype]
   (if (= lhs-dtype rhs-dtype)
     lhs-dtype
     (let [lhs-types (get type-graph lhs-dtype #{:object})
           rhs-types (type-graph rhs-dtype #{:object})
           res (set/intersection lhs-types rhs-types)
           n-res (count res)]
       (cond
         (= 0 n-res)
         :object
         (= 1 n-res)
         (first res)
         :else
         (smallest-datatype res))))))


(def numeric-types (set (map first type-graph-data)))

(defn numeric-type?
  [dtype]
  (boolean (numeric-types dtype)))
