(ns tech.v3.datatype.argtypes)


(defn argtype
  [data]
  (cond
    (or (number? data) (string? data) (boolean? data)) :scalar
    (or (instance? js/Array data) (indexed? data)) :reader
    :else :scalar))


(defn scalar?
  [data]
  (= :scalar (argtype data)))
