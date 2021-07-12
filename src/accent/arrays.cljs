(ns accent.arrays
  "Taken from https://github.com/skrat/accent/blob/master/src/accent/arrays.cljs"
  (:refer-clojure :exclude [concat]))

(def all-array-types
  [js/Int8Array
   js/Uint8Array
   js/Uint8ClampedArray
   js/Int16Array
   js/Uint16Array
   js/Int32Array
   js/Uint32Array
   js/Float32Array
   js/Float64Array])


(defn extend-array-type
  [t]
  (extend-type t
    ISequential
    ISeqable
    (-seq [array] (array-seq array))
    ISeq
    (-first [array] (aget array 0))
    (-rest  [array] (.subarray array 1))
    IIndexed
    (-nth
      ([array n]
       (aget array n))
      ([array n not-found]
       (if (< n (count array))
         (aget array n)
         not-found)))
    ICounted
    (-count [array] (.-length array))
    IReduce
    (-reduce
      ([array f] (array-reduce array f))
      ([array f start] (array-reduce array f start)))
    IPrintWithWriter
    (-pr-writer [array writer opts]
      (-write writer (str "#"
                          (.. array -constructor -name)
                          (array-seq array))))))


(doseq [t all-array-types]
  (extend-array-type t))

(defn int8
  "Creates a native Int8Array for a given `collection`."
  [collection]
  (js/Int8Array. (clj->js collection)))

(defn unsigned-int8
  "Creates a native Uint8Array for a given `collection`."
  [collection]
  (js/Uint8Array. (clj->js collection)))

(defn unsigned-int8-clamped
  "Creates a native Uint8ClampedArray for a given `collection`."
  [collection]
  (js/Uint8ClampedArray. (clj->js collection)))

(defn int16
  "Creates a native Int16Array for a given `collection`."
  [collection]
  (js/Int16Array. (clj->js collection)))

(defn unsigned-int16
  "Creates a native Uint16Array for a given `collection`."
  [collection]
  (js/Uint16Array. (clj->js collection)))

(defn int32
  "Creates a native Int32Array for a given `collection`."
  [collection]
  (js/Int32Array. (clj->js collection)))

(defn unsigned-int32
  "Creates a native Uint32Array for a given `collection`."
  [collection]
  (js/Uint32Array. (clj->js collection)))

(defn float32
  "Creates a native Float32Array for a given `collection`."
  [collection]
  (js/Float32Array. (clj->js collection)))

(defn float64
  "Creates a native Float64Array for a given `collection`."
  [collection]
  (js/Float64Array. (clj->js collection)))

(defn typed-array?
  "Tests whether a given `value` is a typed array."
  [value]
  (some #{(type value)} all-array-types))

(defn ->clj
  [collection]
  (if (typed-array? collection)
    (js->clj (js/Array.apply nil collection))
    collection))

(defn concat
  [a & as]
  (let [all     (cons a as)
        sizes   (map count all)
        outsize (reduce + sizes)
        out     (js* "new a.constructor(outsize)")]
    (loop [arrays all
           offset 0]
      (if-not (zero? (count arrays))
        (let [array (first arrays)
              size  (count array)]
          (.set out array offset)
          (recur (rest arrays) (+ offset size)))
        out))))
