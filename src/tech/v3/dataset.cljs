(ns tech.v3.dataset
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.argops :as argops]
            [tech.v3.datatype.arrays :as arrays]
            [tech.v3.dataset.impl.dataset :as ds-impl]
            [tech.v3.dataset.impl.column :as col-impl]
            [tech.v3.dataset.protocols :as ds-proto]
            [tech.v3.dataset.io.column-parsers :as col-parsers]
            [base64-js :as b64]
            [cognitect.transit :as t]
            [clojure.set :as set])
  (:refer-clojure :exclude [concat update]))


(defn options->parser-fn
  [options]
  (let [parse-map* (atom {})
        key-fn (get options :key-fn identity)
        opt-parse-fn (:parser-fn options)]
    {:parser-fn
     (fn [colname]
       (let [colname (key-fn colname)]
         (if-let [retval (@parse-map* colname)]
           retval
           (let [retval
                 (cond
                   (nil? opt-parse-fn)
                   (col-parsers/promotional-object-parser colname)
                   (keyword? opt-parse-fn)
                   (col-parsers/fixed-type-parser colname opt-parse-fn)
                   :else
                   (if-let [parser-type (get opt-parse-fn colname)]
                     (col-parsers/fixed-type-parser colname parser-type)
                     (col-parsers/promotional-object-parser colname)))]
             (swap! parse-map* assoc colname retval)
             retval))))
     :parse-map* parse-map*}))


(defn- parse-map->dataset
  [parse-map options]
  (let [ary-data (vals parse-map)
        max-rc (apply max 0 (map count ary-data))]
    (ds-impl/new-dataset (select-keys options [:name :dataset-name])
                         (map #(col-parsers/-finalize % max-rc) ary-data))))


(defn- parse-mapseq
  [data options]
  (let [{:keys [parser-fn parse-map*]} (options->parser-fn options)]
    (->> data
         (map-indexed
          (fn [rowidx data]
            (doseq [[k v] data]
              (let [parser (parser-fn k)]
                (col-parsers/-add-value! parser rowidx v)))))
         (doall))
    (parse-map->dataset @parse-map* options)))


(defn- parse-colmap
  "Much faster/easier pathway"
  [data options]
  (let [{:keys [parser-fn parse-map*]} (options->parser-fn options)]
    (doseq [[k v] data]
      (let [parser (parser-fn k)]
        (dtype/add-all! parser v)))
    (parse-map->dataset @parse-map* options)))


(defn ->dataset
  "Convert either a sequence of maps or a map of columns into a dataset.
  Options are similar to the jvm version of tech.v3.dataset in terms of
  parser-fn."
  ([data options]
   (if (nil? data)
     (ds-impl/new-dataset options)
     (cond
       (and (sequential? data) (map? (first data)))
       (parse-mapseq data options)
       (map? data)
       (parse-colmap data options)
       :else
       (throw (js/Error. "Unrecognized value for ->dataset")))))
  ([data] (->dataset data nil))
  ([] (ds-impl/new-dataset)))

(defn ->>dataset
  ([options data]
   (->dataset data options))
  ([data]
   (->dataset data)))


(defn dataset?
  [ds]
  (when ds
    (ds-proto/-is-dataset? ds)))


(defn missing
  [ds-or-col]
  ;;The base missing sets are js sets but interacting with js-sets in Clojure
  ;;is for the birds.
  (set (ds-proto/-missing ds-or-col)))


(defn row-count
  [ds-or-col]
  (if (nil? ds-or-col)
    0
    (ds-proto/-row-count ds-or-col)))


(defn column-count
  [ds]
  (if (nil? ds)
    0
    (ds-proto/-column-count ds)))


(defn columns
  [ds]
  (vals ds))


(defn column-names
  [ds]
  (keys ds))


(defn column
  [ds k]
  (if-let [col (ds k)]
    col
    (throw (js/Error. (str "Unable to find column " k)))))


(defn rows
  "Get a sequence of maps from a dataset"
  [ds]
  (let [cols (vals ds)
        n-rows (row-count ds)]
    (vary-meta
     (dtype/reify-reader
      n-rows
      :object
      (fn [idx]
        (->> (map #(vector (name %) (% idx)) cols)
             (into {}))))
     assoc :simple-print? true)))


(defn row-at
  [ds idx]
  (nth (rows ds) idx))


(defn select-rows
  [ds rowidxs]
  (ds-proto/-select-rows ds (if (or (set? rowidxs)
                                    (instance? js/Set rowidxs))
                              (dtype/set->ordered-indexes rowidxs)
                              rowidxs)))


(defn remove-rows
  [ds rowidxs]
  (let [sdata (if (instance? js/Set rowidxs)
                rowidxs
                (dtype/->js-set rowidxs))
        newidxes (dtype/make-list :int32)]
    (dotimes [idx (row-count ds)]
      (when-not (.has sdata idx)
        (dtype/add! idx newidxes)))
    (select-rows ds rowidxs)))


(defn select-columns
  [ds colnames]
  (ds-proto/-select-columns ds colnames))


(defn remove-columns
  [ds colnames]
  (let [colnames (set colnames)
        ds-colnames (set (column-names ds))
        keep-cols (set/difference ds-colnames colnames)]
    (select-columns ds (filter keep-cols (column-names ds)))))


(defn rename-columns
  [ds rename-map]
  (->> (columns ds)
       (map (fn [col]
              (let [cname (get rename-map (name col) (name col))]
                (vary-meta col :name cname))))
       (ds-impl/new-dataset (meta ds))))


(defn select
  [ds cols rows]
  (-> (select-columns ds cols)
      (select-rows rows)))


(defn filter-column
  [ds colname pred]
  (let [coldata (column ds colname)]
    (select-rows ds (argops/argfilter pred coldata))))


(defn sort-by-column
  [ds colname & [sort-op]]
  (let [coldata (column ds colname)]
    (select-rows ds (argops/argsort sort-op coldata))))


(defn group-by-column
  [ds colname]
  (let [coldata (column ds colname)
        group-map (argops/arggroup coldata)]
    (->> group-map
         (map (fn [[k v]]
                [k (select-rows ds v)]))
         (into {}))))


(defn unique-by-column
  "Unique-by taking first"
  [ds colname]
  (let [coldata (column ds colname)
        passidx (dtype/make-list :int32)]
    (if (dtype/numeric-type? (dtype/elemwise-datatype coldata))
      (let [seen (js/Set.)]
        (dtype/indexed-iterate!
         (fn [idx val]
           (when-not (.has seen val) (dtype/add! passidx idx))
           (.add seen val))
         coldata))
      (let [n-elems (count coldata)]
        (loop [idx 0
               seen #{}]
          (when (< idx n-elems)
            (let [val (coldata idx)]
              (when-not (seen val)
                (dtype/add! passidx idx))
              (recur (unchecked-inc idx) (conj seen val)))))))
    (select-rows ds passidx)))


(defn concat
  "All datasets must have the same column names.  This is a copying concatenation so the
  result will be realized."
  [ds & args]
  (if-not (seq args)
    ds
    (when-let [ds-list (->> (cljs.core/concat [ds] args)
                            (remove #(== 0 (row-count %)))
                            (seq))]
      (if (== (count ds-list) 1)
        (first ds-list)
        (let [colnames (column-names (first ds-list))
              n-rows (apply + (map row-count ds-list))
              col-dtypes (->> colnames
                              ;;force missing column errors right here.
                              (mapv (fn [colname]
                                      (reduce casting/widest-datatype
                                              (map (comp dtype/elemwise-datatype
                                                         #(column % colname))
                                                   ds-list)))))]
          (->> (map (fn [colname coldtype]
                      (let [container (col-impl/make-container coldtype)
                            missing (js/Set.)]
                        (dtype/ensure-capacity! container n-rows)
                        (doseq [ds ds-list]
                          (let [ds-col (column ds colname)
                                offset (count container)]
                            (dtype/iterate! #(.add missing (+ offset %))
                                            (ds-proto/-missing ds-col))
                            (dtype/add-all! container ds-col)))
                        #:tech.v3.dataset{:data container
                                          :missing missing
                                          :name colname}))
                    colnames col-dtypes)
               (ds-impl/new-dataset (meta ds))))))))


(defn merge-by-column
  "Merge rows assuming left, right have the same columns.  Left is taken first then
  any right not appear with left are appended.  This is far less general but much
  faster than a join operation; it is useful for merging timeseries data."
  [lhs rhs colname]
  (->> (column lhs colname)
       (dtype/->set)
       (dtype/set-predicate-complement)
       (filter-column rhs colname)
       (concat lhs)))


(defn- simple-lerp
  [col-dtype lhs rhs n-missing]
  (cond
    (and (nil? lhs) (nil? rhs)) (repeat n-missing 0)
    (nil? lhs) (repeat n-missing rhs)
    (nil? rhs) (repeat n-missing lhs)
    :else
    (let [val-rng (- rhs lhs)]
      (dtype/reify-reader n-missing col-dtype
                          (fn [idx]
                            (let [rel-idx (/ (double (inc idx)) (+ n-missing 1))]
                              (+ lhs (* rel-idx val-rng))))))))


(defn replace-missing
  "Replace missing values in dataset.
  * colnames one or more columns to run replace cmd
  * replace-cmd - one of `:first` `:last` `:lerp` `[:value val]` ifn

  If replace-cmd is an ifn it will be given the column-datatype first and last arguments
  in the missing span and the number of missing elements.  Either the first or last may be
  nil if the missing span is at the beginning or end.  In the case where all values are
  missing both arguments may be nil."

  [ds colnames & [replace-cmd]]
  (let [colnames (if-not (sequential? colnames)
                   (if (= :all colnames)
                     (column-names ds)
                     [colnames])
                   colnames)
        replace-cmd (or replace-cmd :first)
        replace-fn
        (case replace-cmd
          :first (fn [col-dtype lhs rhs n-missing]
                   (repeat n-missing
                           (if-let [retval (or lhs rhs)]
                             retval
                             (if (dtype/numeric-type? col-dtype)
                               0
                               (throw (js/Error. "Entire column is missing :first is selected"))))))
          :last (fn [col-dtype lhs rhs n-missing]
                  (repeat n-missing
                          (if-let [retval (or rhs lhs)]
                            retval
                            (if (dtype/numeric-type? col-dtype)
                              0
                              (throw (js/Error. "Entire column is missing :last is selected"))))))
          :lerp simple-lerp
          (cond
            (fn? replace-cmd) replace-cmd
            (= :value (first replace-cmd))
            (fn [dt lhs rhs n-missing]
              (repeat n-missing (second replace-cmd)))))]
    (reduce
     (fn [ds colname]
       (let [coldata (column ds colname)
             colmissing (ds-proto/-missing coldata)
             n-elems (count coldata)
             col-dtype (dtype/elemwise-datatype coldata)]
         (if (= 0 (count colmissing))
           ds
           (let [new-data (col-impl/make-container col-dtype)]
             (dtype/ensure-capacity! new-data n-elems)
             (loop [idx 0
                    n-missing 0
                    lhs nil]
               (if (< idx n-elems)
                 (let [cur-missing? (.has colmissing idx)
                       n-missing (long (if cur-missing?
                                         (unchecked-inc n-missing)
                                         n-missing))
                       end-run? (and (not cur-missing?)
                                     (not= 0 n-missing))
                       lhs (if (and cur-missing?
                                    (not lhs)
                                    (> idx 0))
                             (coldata (unchecked-dec idx))
                             lhs)
                       cur-val (when-not cur-missing? (coldata idx))]
                   (when end-run?
                     (dtype/add-all! new-data
                                     (replace-fn col-dtype lhs cur-val
                                                 n-missing)))
                   (when-not cur-missing?
                     (dtype/add! new-data cur-val))
                   (recur (unchecked-inc idx)
                          (if cur-missing? n-missing 0)
                          (when cur-missing? lhs)))
                 ;;end condition, check n-missing
                 (when-not (== 0 n-missing)
                   (dtype/add-all! new-data (replace-fn col-dtype lhs nil n-missing)))))
             ;;Use explicit pathway to ensure we do not re-scan data for any reason.
             (assoc ds colname #:tech.v3.dataset{:data new-data
                                                 :missing (js/Set.)
                                                 :name colname
                                                 :metadata (meta coldata)})))))
     ds colnames)))


(defn filter-dataset
  "Filter the columns of the dataset returning a new dataset.  This pathway is
  designed to work with the tech.v3.dataset.column-filters namespace.

  * If filter-fn-or-ds is a dataset, it is returned.
  * If filter-fn-or-ds is sequential, then select-columns is called.
  * If filter-fn-or-ds is :all, all columns are returned
  * If filter-fn-or-ds is an instance of IFn, the dataset is passed into it."
  [dataset filter-fn-or-ds]
  (cond
    (dataset? filter-fn-or-ds)
    filter-fn-or-ds
    (sequential? filter-fn-or-ds)
    (select-columns dataset filter-fn-or-ds)
    (or (nil? filter-fn-or-ds)
        (= :all filter-fn-or-ds))
    dataset
    (or (string? filter-fn-or-ds) (keyword? filter-fn-or-ds))
    (select-columns dataset filter-fn-or-ds)
    (fn? filter-fn-or-ds)
    (filter-fn-or-ds dataset)
    :else
    (throw (js/Error. (str"Unrecoginzed filter mechanism: " filter-fn-or-ds)))))


(defn update
  "Update this dataset.  Filters this dataset into a new dataset,
  applies update-fn, then merges the result into original dataset.

  This pathways is designed to work with the tech.v3.dataset.column-filters namespace.


  * `filter-fn-or-ds` is a generalized parameter.  May be a function,
     a dataset or a sequence of column names.
  *  update-fn must take the dataset as the first argument and must return
     a dataset.

```clojure
(ds/bind-> (ds/->dataset dataset) ds
           (ds/remove-column \"Id\")
           (ds/update cf/string ds/replace-missing-value \"NA\")
           (ds/update-elemwise cf/string #(get {\"\" \"NA\"} % %))
           (ds/update cf/numeric ds/replace-missing-value 0)
           (ds/update cf/boolean ds/replace-missing-value false)
           (ds/update-columnwise (cf/union (cf/numeric ds) (cf/boolean ds))
                                 #(dtype/elemwise-cast % :float64)))
```"
  [lhs-ds filter-fn-or-ds update-fn & args]
  (let [filtered-ds (filter-dataset lhs-ds filter-fn-or-ds)]
    (merge lhs-ds (apply update-fn filtered-ds args))))


(defn column-map
  "Produce a new (or updated) column as the result of mapping a fn over columns.

  * `dataset` - dataset.
  * `result-colname` - Name of new (or existing) column.
  * `map-fn` - function to map over columns.  Same rules as `tech.v3.datatype/emap`.
  * `res-dtype-or-opts` - If not given result is scanned to infer missing and datatype.
  If using an option map, options are described below.
  * `filter-fn-or-ds` - A dataset, a sequence of columns, or a `tech.v3.datasets/column-filters`
     column filter function.  Defaults to all the columns of the existing dataset.

  Returns a new dataset with a new or updated column.

  Options:

  * `:datatype` - Set the dataype of the result column.  If not given result is scanned
  to infer result datatype and missing set.
  * `:missing-fn` - if given, columns are first passed to missing-fn as a sequence and
  this dictates the missing set.  Else the missing set is by scanning the results
  during the inference process. See `tech.v3.dataset.column/union-missing-sets` and
  `tech.v3.dataset.column/intersect-missing-sets` for example functions to pass in
  here.

  Examples:


```clojure

  ;;From the tests --

  (let [testds (ds/->dataset [{:a 1.0 :b 2.0} {:a 3.0 :b 5.0} {:a 4.0 :b nil}])]
    ;;result scanned for both datatype and missing set
    (is (= (vec [3.0 6.0 nil])
           (:b2 (ds/column-map testds :b2 #(when % (inc %)) [:b]))))
    ;;result scanned for missing set only.  Result used in-place.
    (is (= (vec [3.0 6.0 nil])
           (:b2 (ds/column-map testds :b2 #(when % (inc %))
                               {:datatype :float64} [:b]))))
    ;;Nothing scanned at all.
    (is (= (vec [3.0 6.0 nil])
           (:b2 (ds/column-map testds :b2 #(inc %)
                               {:datatype :float64
                                :missing-fn ds-col/union-missing-sets} [:b]))))
    ;;Missing set scanning causes NPE at inc.
    (is (thrown? Throwable
                 (ds/column-map testds :b2 #(inc %)
                                {:datatype :float64}
                                [:b]))))

  ;;Ad-hoc repl --

user> (require '[tech.v3.dataset :as ds]))
nil
user> (def ds (ds/->dataset \"test/data/stocks.csv\"))
#'user/ds
user> (ds/head ds)
test/data/stocks.csv [5 3]:

| symbol |       date | price |
|--------|------------|-------|
|   MSFT | 2000-01-01 | 39.81 |
|   MSFT | 2000-02-01 | 36.35 |
|   MSFT | 2000-03-01 | 43.22 |
|   MSFT | 2000-04-01 | 28.37 |
|   MSFT | 2000-05-01 | 25.45 |
user> (-> (ds/column-map ds \"price^2\" #(* % %) [\"price\"])
          (ds/head))
test/data/stocks.csv [5 4]:

| symbol |       date | price |   price^2 |
|--------|------------|-------|-----------|
|   MSFT | 2000-01-01 | 39.81 | 1584.8361 |
|   MSFT | 2000-02-01 | 36.35 | 1321.3225 |
|   MSFT | 2000-03-01 | 43.22 | 1867.9684 |
|   MSFT | 2000-04-01 | 28.37 |  804.8569 |
|   MSFT | 2000-05-01 | 25.45 |  647.7025 |



user> (def ds1 (ds/->dataset [{:a 1} {:b 2.0} {:a 2 :b 3.0}]))
#'user/ds1
user> ds1
_unnamed [3 2]:

|  :b | :a |
|----:|---:|
|     |  1 |
| 2.0 |    |
| 3.0 |  2 |
user> (ds/column-map ds1 :c (fn [a b]
                              (when (and a b)
                                (+ (double a) (double b))))
                     [:a :b])
_unnamed [3 3]:

|  :b | :a |  :c |
|----:|---:|----:|
|     |  1 |     |
| 2.0 |    |     |
| 3.0 |  2 | 5.0 |
user> (ds/missing (*1 :c))
{0,1}
```"
  ([dataset result-colname map-fn res-dtype-or-opts filter-fn-or-ds]
   (let [opt-map (if (keyword? res-dtype-or-opts)
                   {:datatype res-dtype-or-opts}
                   (or res-dtype-or-opts {}))
         res-dtype (opt-map :datatype :object)]
     (update dataset filter-fn-or-ds
             (fn [update-ds]
               (let [cols (columns update-ds)
                     coldata (apply dtype/emap map-fn res-dtype (columns update-ds))
                     missing (when-let [missing-fn (opt-map :missing-fn)] (missing-fn cols))]
                 (if missing
                   (assoc update-ds result-colname
                          #:tech.v3.dataset{:data coldata
                                            :missing missing
                                            :name result-colname})
                   (assoc update-ds result-colname coldata)))))))
  ([dataset result-colname map-fn filter-fn-or-ds]
   (column-map dataset result-colname map-fn nil filter-fn-or-ds))
  ([dataset result-colname map-fn]
   (column-map dataset result-colname map-fn nil (column-names dataset))))


(defn union-missing-sets
  "Union the missing sets of the columns"
  [col-seq]
  (reduce dtype/set-or (map ds-proto/-missing col-seq)))


(defn intersect-missing-sets
  "Intersect the missing sets of the columns"
  [col-seq]
  (reduce dtype/set-and (map ds-proto/-missing col-seq)))


(defn- string-col->data
  [col]
  ;;make a new string table.
  (let [strmap (js/Map.)
        strtable (js/Array.)
        indexes (js/Array.)]
    (dtype/indexed-iterate!
     (fn [idx strval]
       (.push indexes
              (when strval
                (if-let [cur-idx (.get strmap strval)]
                  cur-idx
                  (let [cur-idx (count strtable)]
                    (.push strtable strval)
                    (.set strmap strval cur-idx)
                    cur-idx))))) col)
    {:strtable strtable
     :indexes indexes}))


(defn- numeric-data->b64
  [data]
  (-> (clone data)
      (dtype/ensure-typed-array)
      (aget "buffer")
      (js/Uint8Array.)
      (b64/fromByteArray)))


(defn- col->data
  [col]
  (let [col-dt (dtype/elemwise-datatype col)]
    {:metadata (meta col)
     :missing (vec (dtype/set->ordered-indexes
                    (ds-proto/-missing col)))
     :data
     (cond
       (dtype/numeric-type? col-dt)
       (numeric-data->b64 (aget col "buf"))
       (= :boolean col-dt)
       (numeric-data->b64 (dtype/make-container :uint8 (aget col "buf")))
       (= :string col-dt)
       (string-col->data col)
       :else
       (dtype/as-js-array (dtype/make-container :object (aget col "buf"))))}))


(defn dataset->data
  "Convert a dataset into a pure data datastructure save for transit or direct json
  serialization.  Uses base64 encoding of numeric data."
  [ds]
  {:metadata (meta ds)
   :flavor :cljs
   :version 1
   :columns (mapv col->data (columns ds))})


(defn- str-data->coldata
  [{:keys [strtable indexes]}]
  (let [coldata (dtype/make-container :string (count indexes))
        agetable (dtype/as-agetable coldata)]
    (dotimes [idx (count indexes)]
      (aset agetable idx (nth strtable (nth indexes idx))))
    coldata))


(defn data->dataset
  [ds-data]
  (when-not (and (contains? ds-data :metadata)
                 (contains? ds-data :columns))
    (throw (js/Error. "This does not seem like dataset data, missing required keys")))
  (->> (:columns ds-data)
       (map
        (fn [{:keys [metadata missing data]}]
          (let [dtype (:datatype metadata)]
            #:tech.v3.dataset{:metadata metadata
                              :missing (dtype/->js-set missing)
                              :data
                              (cond
                                (dtype/numeric-type? dtype)
                                (let [bdata (-> (b64/toByteArray data)
                                                (aget "buffer"))]
                                  (case dtype
                                    :int8 (js/Int8Array. bdata)
                                    :uint8 bdata
                                    :int16 (js/Int16Array. bdata)
                                    :uint16 (js/Uint16Array. bdata)
                                    :int32 (js/Int32Array. bdata)
                                    :uint32 (js/Uint32Array. bdata)
                                    :float32 (js/Float32Array. bdata)
                                    :float64 (js/Float64Array. bdata)))
                                (= :boolean dtype)
                                (arrays/make-boolean-array (b64/toByteArray data))
                                (= :string dtype)
                                (str-data->coldata data)
                                :else
                                (if (instance? js/Array data)
                                  (arrays/make-typed-buffer data dtype)
                                  (dtype/as-agetable (dtype/make-container dtype data))))
                              :name (:name metadata)})))
       (ds-impl/new-dataset (:metadata ds-data))))

(def writer* (delay (t/writer :json)))
(def reader* (delay (t/reader :json)))


(defn dataset->json
  [ds]
  (.write @writer* (dataset->data ds)))


(defn json->dataset
  [json-data]
  (data->dataset (.read @reader* json-data)))


(comment
  (do
    (def test-data (repeatedly 5000 #(hash-map
                                      :time (rand)
                                      :temp (rand)
                                      :valid? (if (> (rand) 0.5) true false))))


    (def test-ds (->dataset test-data))
    (def min-ds (select-rows test-ds (range 20))))


  (def ignored (time (->> (cljs.core/concat test-data test-data)
                          (sort-by :time)
                          (dedupe)
                          (count))))

  (def ignored (time (.write @writer* test-data)))

  (def ignored (time (dataset->json test-ds)))

  (def ignored (let [data (.write @writer* test-data)]
                 (time (.read @reader* data))))

  (def ignored (let [data (dataset->json test-ds)]
                 (time (json->dataset data))))

  )
