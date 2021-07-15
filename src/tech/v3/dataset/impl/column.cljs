(ns tech.v3.dataset.impl.column
  "Column implementation and defaults"
  (:require [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype :as dtype]
            [tech.v3.dataset.string-table :as strt]))


(defn datatype->missing-value
  [dtype]
  ;;these need to match the jvm values
  (case dtype
    :boolean false
    :int8 -128
    :int16 -32768
    :int32 -2147483648
    ;;really not enough bits for this but whatevs
    :int64 -9223372036854775808
    :float32 ##NaN
    :float64 ##NaN
    (if (casting/numeric-type? dtype)
      0
      nil)))


(defn make-container
  [dtype]
  (if (= :string dtype)
    (strt/make-string-table)
    (dtype/make-list dtype)))
