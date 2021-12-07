(ns tech.v3.dataset.columnwise-map
  (:require [tech.v3.datatype.protocols :as dt-proto]))

(deftype ColAryRowIter [col-ary n-cols ^:mutable col-idx row-idx]
  Object
  (hasNext [_iter] (< col-idx n-cols))
  (next [_iter]
    (let [col-val (col-ary col-idx)]
      (set! col-idx (inc col-idx))
      (MapEntry. (name col-val) (col-val row-idx) nil))))


(deftype ColumnwiseMap [meta col-ary colname->col row-idx ^:mutable __hash]
  Object
  (toString [coll]
    (pr-str* coll))
  (equiv [this other]
    (-equiv this other))

  ;; EXPERIMENTAL: subject to change
  (keys [coll]
    (es6-iterator (keys coll)))
  (entries [coll]
    (es6-entries-iterator (seq coll)))
  (values [coll]
    (es6-iterator (vals coll)))
  (has [coll k]
    (contains? coll k))
  (get [coll k not-found]
    (-lookup coll k not-found))
  (forEach [coll f]
    (doseq [[k v] coll]
      (f v k)))

  ICloneable
  (-clone [coll] (-> (into {} coll) (with-meta meta)))

  IIterable
  (-iterator [_coll]
    (ColAryRowIter. col-ary (count col-ary) 0 row-idx))

  IWithMeta
  (-with-meta [coll new-meta]
    (if (identical? new-meta meta)
      coll
      (ColumnwiseMap. new-meta col-ary colname->col row-idx __hash)))

  IMeta
  (-meta [coll] meta)

  ICollection
  (-conj [coll entry]
    (-conj (into {} coll) entry))

  IEmptyableCollection
  (-empty [coll] (-with-meta (.-EMPTY PersistentHashMap) meta))

  IEquiv
  (-equiv [coll other] (equiv-map coll other))

  IHash
  (-hash [coll] (caching-hash coll hash-unordered-coll __hash))

  ISeqable
  (-seq [coll]
    (when (pos? (count col-ary))
      (map (fn [col] (MapEntry. (name col) (col row-idx) nil)) col-ary)))

  ICounted
  (-count [coll] (count col-ary))

  ILookup
  (-lookup [coll k]
    (-lookup coll k nil))

  (-lookup [coll k not-found]
    (if-let [idx (colname->col k)]
      ((col-ary idx) row-idx)
      not-found))

  IAssociative
  (-assoc [coll k v]
    (persistent! (assoc! (transient coll) k v)))

  (-contains-key? [coll k]
    (-contains-key? colname->col k))

  IFind
  (-find [coll k]
    (when-let [col-idx (colname->col k)]
      (MapEntry. k ((col-ary col-idx) row-idx) nil)))

  IMap
  (-dissoc [coll k]
    (if-let [col-idx (colname->col k)]
      (let [left-vec (subvec col-ary 0 col-idx)
            right-vec (subvec col-ary (inc col-idx))
            new-ary (vec (concat left-vec right-vec))
            new-colmap (->> new-ary
                            (map-indexed #(vector (name %2) %1))
                            (into {}))]
        (ColumnwiseMap. meta new-ary new-colmap row-idx nil))
      coll))

  IKVReduce
  (-kv-reduce [coll f init]
    (let [n-cols (count col-ary)]
      (loop [idx 0
             init init]
        (if (< idx n-cols)
          (let [col (col-ary idx)
                init (f init (name col) (col row-idx))]
            (if (reduced? init)
              init
              (recur (inc idx) init)))
          init))))

  IFn
  (-invoke [coll k]
    (-lookup coll k))

  (-invoke [coll k not-found]
    (-lookup coll k not-found))

  IEditableCollection
  (-as-transient [coll]
    (let [n-cols (count col-ary)]
      (loop [retval (transient {})
             idx 0]
        (if (< idx n-cols)
          (let [col (col-ary idx)]
            (recur (assoc! retval (name col) (col row-idx)) (inc idx)))
          retval))))
  IPrintWithWriter
  (-pr-writer [cmap writer opts]
    (-pr-writer (into {} cmap) writer opts))
  dt-proto/PDatatype
  (-datatype [this] :persistent-map))


(defn columnwise-map
  [col-ary colname->col row-idx]
  (ColumnwiseMap. nil col-ary colname->col row-idx nil))
