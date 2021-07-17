(ns tech.v3.dataset.impl.dataset
  (:require [tech.v3.datatype.argtypes :as argtypes]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.protocols :as dt-proto]
            [tech.v3.dataset.impl.column :as col-impl]
            [tech.v3.dataset.io.column-parsers :as col-parsers]
            [tech.v3.dataset.protocols :as ds-proto]
            [tech.v3.dataset.format-sequence :as fmt]
            [clojure.string :as str]))


(defn- extend-data
  [data n-rows]
  (let [n-data (count data)]
    (cond
      (< n-data n-rows)
      (let [dtype (dtype/elemwise-datatype data)
            missing-val (col-impl/datatype->missing-value dtype)
            new-data (dtype/make-container dtype n-rows)]
        (dtype/set-value! new-data 0 data)
        (dt-proto/-set-constant! new-data n-data (- n-rows n-data) missing-val)
        new-data)
      (> n-data n-rows)
      (dt-proto/-sub-buffer data 0 n-rows)
      :else
      data)))


(defn- scan-missing
  [data]
  (let [missing (js/Set.)
        missing-val (if (casting/numeric-type?
                         (dtype/elemwise-datatype data))
                      ##NaN
                      nil)]
    (dtype/indexed-iterate!
     #(when (= missing-val %2)
        (.add missing %1))
     data)
    missing))


(defn ->column
  [data & [colname n-rows]]
  (if (ds-proto/-is-column? data)
    data
    (let [darg (argtypes/argtype data)
          dtype (dtype/elemwise-datatype data)
          colname (or colname
                      (:tech.v3.dataset/name data)
                      :_unnamed)
          colmap (cond
                   (and (map? data) (contains? data :tech.v3.dataset/data))
                   data
                   (or (= :iterable darg) (= :object dtype))
                   (do
                     (let [n-rows (or n-rows (when (dtype/counted? data)
                                               (count data)))]
                       (when-not n-rows (throw (js/Error. "Potentially infinite iteration")))
                       (let [pparser (col-parsers/promotional-object-parser colname)]
                         (dtype/add-all! pparser (take n-rows data))
                         (col-parsers/-finalize pparser n-rows))))
                   (= :reader darg)
                   (let [n-rows (or n-rows (count data))]
                     (let [data (extend-data data n-rows)]
                       #:tech.v3.dataset{:data data
                                         :missing (scan-missing data)
                                         :name colname}))
                   (= :scalar darg)
                   (let [container (dtype/make-container dtype n-rows)
                         missing? (= data (col-impl/datatype->missing-value dtype))
                         missing (js/Set.)]
                     (dt-proto/-set-constant! container 0 n-rows data)
                     (when missing? (dotimes [idx n-rows] (.add missing idx)))
                     #:tech.v3.dataset{:data container
                                       :missing missing
                                       :name colname}))]
      (let [{:keys [tech.v3.dataset/name tech.v3.dataset/data
                    tech.v3.dataset/missing tech.v3.dataset/metadata]} colmap]
        (let [missing (or missing (js/set.))
              name (or colname name :_unnamed)]
          (col-impl/new-column data missing (casting/numeric-type?
                                             (dtype/elemwise-datatype data))
                               (assoc metadata :name name)))))))


(declare cols->str)



(deftype Dataset [col-ary colname->col metadata]
  ICounted
  (-count [this] (count col-ary))

  ICloneable
  (-clone [this]
    (Dataset. (mapv clone col-ary) colname->col metadata))

  IAssociative
  (^boolean -contains-key? [coll k]
   (-contains-key? colname->col k))

  (^clj -assoc [coll k v]
   (let [row-count (ds-proto/-row-count coll)
         v (->column v k (if (== 0 row-count)
                           nil
                           row-count))]
     (if-let [col-idx (get colname->col k)]
       (let [n-col-ary (assoc col-ary col-idx v)]
         (Dataset. n-col-ary colname->col metadata))
       (let [col-idx (count col-ary)
             n-col-ary (conj col-ary v)
             n-colname->col (assoc colname->col k col-idx)]
         (Dataset. n-col-ary n-colname->col metadata)))))

  IMap
  (^clj -dissoc [coll k]
   (if-let [col-idx (get colname->col k)]
     (let [n-col-ary (vec (concat (subvec col-ary 0 col-idx)
                                  (subvec col-ary (inc col-idx))))
           n-col-map (->> (map-indexed (fn [idx col]
                                         [(:name (meta col)) idx]))
                          (into {}))]
       (Dataset. n-col-ary n-col-map metadata))
     coll))

  ILookup
  (-lookup [coll k] (-lookup coll k nil))
  (-lookup [coll k not-found]
    (if-let [col-idx (colname->col k)]
      (col-ary col-idx)
      not-found))
  IFn
  (-invoke [coll k]
    (-lookup coll k))
  (-invoke [coll k not-found]
    (-lookup coll k not-found))
  IMeta
  (-meta [coll] (assoc metadata
                       :row-count (ds-proto/-row-count coll)
                       :column-count (ds-proto/-column-count coll)))
  IWithMeta
  (-with-meta [coll new-meta]
    (Dataset. col-ary colname->col new-meta))

  INamed
  (-name [this] (:name metadata))

  ISeqable
  (-seq [this] (map #(MapEntry. (name %) % nil) col-ary))

  ISeq
  (-first [this] (first col-ary))
  (-rest [this] (rest col-ary))

  IEmptyableCollection
  (-empty [coll] (Dataset. [] {} metadata))

  IPrintWithWriter
  (-pr-writer [array writer opts]
    (-write writer (str "#dataset[" (:name metadata "unnamed")
                        " [" (ds-proto/-row-count array)
                        " " (ds-proto/-column-count array) "]\n"
                        (cols->str col-ary (merge opts metadata))
                        "]")))

  ds-proto/PRowCount
  (-row-count [this]
    (if-let [col (nth col-ary 0 nil)]
      (count col)
      0))

  ds-proto/PColumnCount
  (-column-count [this]
    (count col-ary))

  ds-proto/PDataset
  (-is-dataset? [ds] true)
  (-column [ds colname]
    (if-let [col-idx (colname->col colname)]
      (col-ary col-idx)
      (throw (js/Error. (str "No column named \"" colname "\"")))))

  ds-proto/PMissing
  (-missing [this]
    (let [retval (js/Set.)]
      (doseq [col col-ary]
        (dtype/iterate! #(.add retval %) (ds-proto/-missing col)))
      retval))

  ds-proto/PSelectRows
  (-select-rows [this rowidxes]
    (Dataset. (mapv #(ds-proto/-select-rows % rowidxes) col-ary) colname->col metadata))

  ds-proto/PSelectColumns
  (-select-columns [this colnames]
    (let [n-col-ary (mapv #(ds-proto/-column this %) colnames)
          n-colmap (->> colnames
                        (map-indexed (fn [idx cname]
                                       [cname idx]))
                        (into {}))]
      (Dataset. n-col-ary n-colmap metadata))))


(defn- reader->string-lines
  [reader-data line-policy column-max-width]
  ;;step 1 is to stringify the data
  (let [reader-data (if (casting/numeric-type? (dtype/elemwise-datatype reader-data))
                      (fmt/format-sequence reader-data)
                      (map #(when (not (nil? %)) (pr-str %)) reader-data))]
    ;;step 2 is to format the data respecting multiple line and max-width params.
    (->> reader-data
         (mapv (fn [strval]
                 (if (nil? strval)
                   ""
                   (let [lines (str/split-lines strval)
                         lines (if (number? column-max-width)
                                 (let [width (long column-max-width)]
                                   (->> lines (map (fn [^String line]
                                                     (if (> (count line) width)
                                                       (.substring line 0 width)
                                                       line)))))
                                 lines)]
                     (case line-policy
                       :single
                       [(first lines)]
                       :markdown
                       [(str/join "<br>" lines)]
                       :repl
                       lines))))))))

(defn- rpad-str
  [col-width line]
  (let [n-data (count line)
        n-pad (- (long col-width) n-data)]
    (apply str (concat (repeat n-pad " ") [line]))))


(defn cols->str
  "Convert the dataset values to a string.

Options may be provided in the dataset metadata or may be provided
as an options map.  The options map overrides the dataset metadata.


  * `:print-index-range` - The set of indexes to print.  Defaults to:
    (range *default-table-row-print-length*)
  * `:print-line-policy` - defaults to `:repl` - one of:
     - `:repl` - multiline table - default nice printing for repl
     - `:markdown` - lines delimited by <br>
     - `:single` - Only print first line
  * `:print-column-max-width` - set the max width of a column when printing.
  * `:print-column-types?` - show/hide column datatypes."
  [col-ary & [options]]
  (when (seq col-ary)
    (let [print-index-range (get options :print-index-range (range 25))
          print-line-policy (get options :print-line-policy :repl)
          print-column-max-width (get options :print-column-max-width 32)
          print-column-types? (get options :print-column-types? false)
          col-ary (mapv #(ds-proto/-select-rows % print-index-range) col-ary)
          cnames (map (comp :name meta) col-ary)
          column-names (map #(when % (str %)) cnames)
          column-types (map #(str (when print-column-types? (:datatype (meta %))))
                            col-ary)
          string-columns (map #(reader->string-lines
                                %
                                print-line-policy
                                print-column-max-width)
                              col-ary)

          n-rows (long (count (first col-ary)))
          row-heights (dtype/make-container :int32 (repeat n-rows 1))
          column-widths
          (->> string-columns
               (map (fn [coltype colname coldata]
                      (->> coldata
                           (map-indexed
                            (fn [row-idx lines]
                              ;;Side effecting record row height.
                              (dtype/set-value! row-heights (int row-idx)
                                                (max (int (nth row-heights row-idx))
                                                     (count lines)))
                              (apply max 0 (map count lines))))
                           (apply max (count coltype) (count colname))))
                    column-types
                    column-names))
          spacers (map #(apply str (repeat % "-")) column-widths)
          fmt-row (fn [leader divider trailer row]
                    (str leader
                         (apply str
                                (interpose
                                 divider
                                 (map #(rpad-str %1 %2) column-widths row)))
                         trailer))
          builder (dtype/make-list :object)
          append-line! #(dtype/add! %1 %2)]
      (append-line! builder (fmt-row "| " " | " " |" column-names))
      (when print-column-types? (append-line! builder (fmt-row "| " " | " " |" column-types)))
      (append-line!
       builder
       (apply str
              (concat (mapcat (fn [spacer dtype]
                                (let [numeric? (casting/numeric-type? dtype)]
                                  (concat ["|-"]
                                          spacer
                                          (if numeric?
                                            ":"
                                            "-"))))
                              spacers (map dtype/elemwise-datatype
                                           col-ary))
                      ["|"])))
      (dotimes [idx n-rows]
       (let [row-height (long (nth row-heights idx))]
         (dotimes [inner-idx row-height]
           (let [row-data
                 (->> string-columns
                      (map (fn [c-width column]
                             (let [lines (column idx)]
                               (if (< inner-idx (count lines))
                                 (if (== 1 (count lines))
                                   (nth lines inner-idx)
                                   (->> (nth lines inner-idx)
                                        (rpad-str c-width)))
                                 "")))
                           column-widths))]
             (append-line! builder (fmt-row "| " " | " " |" row-data))))))
      (str/join "\n" builder))))


(defn new-dataset
  ([metadata colseq]
   (let [metadata (if (map? metadata)
                    metadata
                    {:name metadata})
         metadata (update metadata :name #(or % "unnamed"))
         coldata (map (fn [coldata]
                        (or (:tech.v3.dataset/data coldata)
                            coldata))
                      colseq)

         n-rows (or (when (seq colseq)
                      (when-let [rdr-lens
                                 (->> coldata
                                      (map (fn [coldata]
                                             (when (= :reader (argtypes/argtype coldata))
                                               (count coldata))))
                                      (remove nil?)
                                      (seq))]
                        (apply min rdr-lens)))
                    ;;Things that aren't readers have no length or infinite length
                    0)
         any-scalars? (some #(= (argtypes/argtype %) :scalar) coldata)
         n-rows (max n-rows (if any-scalars? 1 0))
         columns (->> colseq
                      (map-indexed (fn [idx coldata]
                                     (let [col (->column coldata nil n-rows)]
                                       ;;unnamed columns get their col index as their name
                                       (if (= :_unnamed (name col))
                                         (vary-meta col assoc :name idx)
                                         col))))
                      vec)
         names (set (map name columns))
         _ (when-not (= (count names)
                        (count columns))
             (throw (js/Error. (str "Multiple columns share the same name: "
                                    (map name columns)))))
         colmap (->> columns
                     (map-indexed (fn [idx col]
                                    [(name col) idx]))
                     (into {}))]
     (Dataset. columns colmap metadata)))
  ([colseq]
   (new-dataset {:name "unnamed"} colseq))
  ([] (new-dataset {:name "unnamed"} nil)))