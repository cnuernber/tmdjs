(ns tech.v3.dataset
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.dataset.impl.dataset :as ds-impl]
            [tech.v3.dataset.protocols :as ds-proto]
            [tech.v3.dataset.io.column-parsers :as col-parsers]))


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
    (ds-impl/new-dataset options (map #(col-parsers/-finalize % max-rc) ary-data))))


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


(defn missing
  [ds-or-col]
  ;;The base missing sets are js sets but interacting with js-sets for most of
  ;;clojure is for the birds.
  (set (ds-proto/-missing ds-or-col)))


(defn row-count
  [ds-or-col]
  (ds-proto/-row-count ds-or-col))


(defn column-count
  [ds]
  (ds-proto/-column-count ds))


(defn columns
  [ds]
  (vals ds))


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
