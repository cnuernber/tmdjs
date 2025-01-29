(ns tech.v3.dataset.impl.dataset
  (:require [tech.v3.datatype.argtypes :as argtypes]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.arrays :as dt-arrays]
            [tech.v3.datatype.protocols :as dt-proto]
            [tech.v3.dataset.impl.column :as col-impl]
            [tech.v3.dataset.io.column-parsers :as col-parsers]
            [tech.v3.dataset.protocols :as ds-proto]
            [tech.v3.datatype.format-sequence :as fmt]
            [tech.v3.dataset.columnwise-map :as cmap]
            [ham-fisted.api :as hamf]
            [ham-fisted.lazy-noncaching :as lznc]
            [clojure.string :as str]))


(declare dataset?)


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


(defn- scan-data
  [data colname n-rows]
  (let [pparser (col-parsers/promotional-object-parser colname)]
    (dtype/add-all! pparser (take n-rows data))
    (vary-meta (col-parsers/-finalize pparser n-rows)
               #(merge (meta data) %))))


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
                   (let [{:keys [tech.v3.dataset/data tech.v3.dataset/force-datatype?]
                          :as data-map} data
                         n-rows (or n-rows (when (dtype/counted? data) (count data)))
                         data-map (if (and (= :object (dtype/elemwise-datatype data))
                                           (not force-datatype?))
                                    (merge data-map
                                           (scan-data data colname n-rows))
                                    data-map)
                         missing (data-map :tech.v3.dataset/missing)
                         missing (if-not missing
                                   (scan-missing (data-map :tech.v3.dataset/data))
                                   missing)]
                     (assoc data-map
                            :tech.v3.dataset/missing missing
                            :tech.v3.dataset/force-datatype? true))
                   (or (= :iterable darg) (= :object dtype))
                   (let [n-rows (or n-rows (when (dtype/counted? data)
                                             (count data)))]
                     (when-not n-rows (throw (js/Error. "Potentially infinite iteration")))
                     (scan-data data colname n-rows))
                   (= :reader darg)
                   (let [n-rows (or n-rows (count data))
                         data (extend-data data n-rows)]
                     #:tech.v3.dataset{:data data
                                       :missing (scan-missing data)
                                       :name colname})
                   (= :scalar darg)
                   (let [container (dtype/make-container dtype n-rows)
                         missing? (= data (col-impl/datatype->missing-value dtype))
                         missing (js/Set.)]
                     (dt-proto/-set-constant! container 0 n-rows data)
                     (when missing? (dotimes [idx n-rows] (.add missing idx)))
                     #:tech.v3.dataset{:data container
                                       :missing missing
                                       :name colname}))]
      (if (ds-proto/-is-column? colmap)
        colmap
        (let [{:keys [tech.v3.dataset/name tech.v3.dataset/data
                      tech.v3.dataset/missing tech.v3.dataset/metadata]} colmap
              missing (or missing (js/Set.))
              name (or colname name :_unnamed)]
          (col-impl/new-column data missing
                               (assoc metadata :name name)
                               (casting/numeric-type?
                                (dtype/elemwise-datatype data))))))))


(declare cols->str)


(declare make-dataset)

(defn- col-ary->map-entries
  [col-ary]
  (let [ne (count col-ary)]
    (when-not (zero? ne)
      (dtype/reify-reader ne (fn [idx]
                               (let [col (col-ary idx)]
                                 (MapEntry. (name col) col nil)))))))

(deftype Dataset [col-ary colname->col metadata]
  Object
  (toString [coll]
    (pr-str* coll))
  (equiv [this other]
    (-equiv this other))
  ICounted
  (-count [_this] (count col-ary))

  ICloneable
  (-clone [_this]
    (make-dataset (mapv clone col-ary) colname->col metadata))

  IAssociative
  (^boolean -contains-key? [_coll k]
   (-contains-key? colname->col k))

  (^clj -assoc [coll k v]
   (let [row-count (ds-proto/-row-count coll)
         v (-> (->column v k (if (== 0 row-count)
                               nil
                               row-count))
               (vary-meta assoc :name k))]
     (if-let [col-idx (get colname->col k)]
       (let [n-col-ary (assoc col-ary col-idx v)]
         (make-dataset n-col-ary colname->col metadata))
       (let [col-idx (count col-ary)
             n-col-ary (conj col-ary v)
             n-colname->col (assoc colname->col k col-idx)]
         (make-dataset n-col-ary n-colname->col metadata)))))
  IMap
  (^clj -dissoc [coll k]
   (if-let [col-idx (get colname->col k)]
     (let [n-col-ary (vec (concat (subvec col-ary 0 col-idx)
                                  (subvec col-ary (inc col-idx))))
           n-col-map (->> n-col-ary
                          (map-indexed (fn [idx col]
                                         [(name col) idx]))
                          (into {}))]
       (make-dataset n-col-ary n-col-map metadata))
     coll))

  ILookup
  (-lookup [coll k] (-lookup coll k nil))
  (-lookup [_coll k not-found]
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
  (-with-meta [_coll new-meta]
    (make-dataset col-ary colname->col new-meta))

  INamed
  (-name [_this] (:name metadata))

  ISeqable
  (-seq [_this]
    (seq (col-ary->map-entries col-ary)))

  ISeq
  (-first [_this] (first col-ary))
  (-rest [_this] (rest col-ary))

  IEmptyableCollection
  (-empty [_coll] (make-dataset [] {} metadata))

  IPrintWithWriter
  (-pr-writer [array writer opts]
    (-write writer (str "#dataset[" (:name metadata "unnamed")
                        " [" (ds-proto/-row-count array)
                        " " (ds-proto/-column-count array) "]\n"
                        (cols->str col-ary (merge opts metadata))
                        "]")))

  ICollection
  (-conj [coll o]
    (when-not (or (ds-proto/-is-column? o)
                  (dataset? o))
      (throw (js/Error. "Only columns or datasets can be conj'd onto a dataset")))
    (if (ds-proto/-is-column? o)
      (assoc coll (name o) o)
      (reduce (fn [coll col]
                (assoc coll (name col) col))
              coll
              (ds-proto/-columns o))))

  IHash
  (-hash [this] (hamf/hash-unordered (into [] (lznc/map
                                               (fn [col]
                                                 [(get (meta col) :name) col])
                                               col-ary))))
  IEquiv
  (-equiv [this other]
    (if (and other
             (dataset? other)
             (= (ds-proto/-row-count this)
                (ds-proto/-row-count other))
             (= (ds-proto/-column-count this)
                (ds-proto/-column-count other)))
      (let [mycols (ds-proto/-columns this)
            othercols (ds-proto/-columns other)]
        (and (= (mapv name mycols)
                (mapv name othercols))
             (= mycols othercols)))
      false))

  IIterable
  (-iterator [_this] (dt-arrays/nth-iter (col-ary->map-entries col-ary)))

  ds-proto/PRowCount
  (-row-count [_this]
    (if-let [col (nth col-ary 0 nil)]
      (count col)
      0))

  ds-proto/PColumnCount
  (-column-count [_this]
    (count col-ary))

  ds-proto/PDataset
  (-column [_ds colname]
    (if-let [col-idx (colname->col colname)]
      (col-ary col-idx)
      (throw (js/Error. (str "No column named \"" colname "\"")))))
  (-columns [_ds] col-ary)
  (-columns-as-map [_ds] (into {} (map #(vector (name %) %)) col-ary))
  (-rows [ds]
    (dtype/reify-reader :persistent-map
                        (ds-proto/-row-count ds)
                        #(cmap/columnwise-map col-ary colname->col %)))
  (-rowvecs [ds]
    (dtype/reify-reader :persistent-vector
                        (ds-proto/-row-count ds)
                        (fn [row-idx ]
                          (dtype/reify-reader (count col-ary)
                                              (fn [col-idx]
                                                ((col-ary col-idx) row-idx))))))
  (-row-at [ds idx]
    (let [rc (ds-proto/-row-count ds)
          idx (if (< idx 0)
                (+ rc idx)
                idx)]
      (when-not (< idx rc)
        (throw (js/Error. (str "row-at index out of range: " idx " >= " rc))))
      (cmap/columnwise-map col-ary colname->col idx)))
  (-rowvec-at [ds idx]
    (let [rc (ds-proto/-row-count ds)
          idx (if (< idx 0)
                (+ rc idx)
                idx)]
      (when-not (< idx rc)
        (throw (js/Error. (str "row-at index out of range: " idx " >= " rc))))
      (dtype/reify-reader (count col-ary) #((col-ary %) idx))))

  ds-proto/PMissing
  (-missing [_this]
    (let [retval (js/Set.)]
      (doseq [col col-ary]
        (dtype/iterate! #(.add retval %) (ds-proto/-missing col)))
      retval))

  ds-proto/PSelectRows
  (-select-rows [this rowidxes]
    (let [rowidxes (col-impl/process-row-indexes (ds-proto/-row-count this) rowidxes)]
      (make-dataset (mapv #(ds-proto/-select-rows % rowidxes) col-ary) colname->col metadata)))

  ds-proto/PSelectColumns
  (-select-columns [this colnames]
    (let [n-col-ary (mapv #(ds-proto/-column this %) colnames)
          n-colmap (->> colnames
                        (map-indexed (fn [idx cname]
                                       [cname idx]))
                        (into {}))]
      (make-dataset n-col-ary n-colmap metadata))))

(defn dataset?
  [x]
  (instance? Dataset x))


(defn- reader->string-lines
  [reader-data line-policy column-max-width]
  ;;step 1 is to stringify the data
  (let [reader-data (if (casting/numeric-type? (dtype/elemwise-datatype reader-data))
                      (fmt/format-sequence reader-data)
                      (map #(when (not (nil? %)) (.toString %)) reader-data))]
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


  * `:print-index-range` - The set of indexes to print.  If an integer then
     is interpreted according to `:print-style`.  Defaults to the integer 10.
  * `:print-style` - Defaults to :first-last.  Options are #{:first-last :first :last}.  In
     the case `:print-index-range` is an integer and the dataset has more than that number of
     rows prints the first N/2 and last N/2 rows or the first N or last N rows.
  * `:print-line-policy` - defaults to `:repl` - one of:
     - `:repl` - multiline table - default nice printing for repl
     - `:markdown` - lines delimited by <br>
     - `:single` - Only print first line
  * `:print-column-max-width` - set the max width of a column when printing.
  * `:print-column-types?` - show/hide column datatypes."
  [col-ary & [options]]
  (when (seq col-ary)
    (let [print-line-policy (get options :print-line-policy :repl)
          print-column-max-width (get options :print-column-max-width 32)
          print-column-types? (get options :print-column-types? false)
          n-rows (count (first col-ary))
          index-range (get options :print-index-range (min n-rows 10))
          print-style (when (number? index-range) (get options :print-style :first-last))
          [index-range ellipses?] (if (number? index-range)
                                    (case print-style
                                      :first-last
                                      (if (> n-rows (long index-range))
                                        (let [start-n (quot (long index-range) 2)
                                              end-start (dec (- n-rows start-n))]
                                          [(vec (concat (range start-n)
                                                        (range end-start n-rows)))
                                           true])
                                        [(range n-rows) false])
                                      :first
                                      [(range (min index-range n-rows)) false]
                                      :last
                                      [(range (max 0 (- n-rows (long index-range))) n-rows) false])
                                    [index-range false])
          col-ary (mapv #(ds-proto/-select-rows % index-range) col-ary)
          cnames (map (comp :name meta) col-ary)
          column-names (map #(when % (str %)) cnames)
          column-types (map #(str (when print-column-types? (:datatype (meta %))))
                            col-ary)
          string-columns (map #(reader->string-lines
                                %
                                print-line-policy
                                print-column-max-width)
                              col-ary)
          string-columns (if ellipses?
                           (let [insert-pos (quot (dtype/ecount index-range) 2)]
                             (->> string-columns
                                  (map (fn [str-col]
                                         (vec (concat (take insert-pos str-col)
                                                      [["..."]]
                                                      (drop insert-pos str-col)))))))
                           string-columns)
          ;;reset n-rows to be correctly shortened n-rows
          n-rows (count (first string-columns))
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
      #_(println (mapv count string-columns) n-rows (vec (first string-columns)))
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


(defn- make-dataset
  "private dataset api"
  [col-ary colmap metadata]
  (Dataset. col-ary colmap metadata))


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
     (make-dataset columns colmap metadata)))
  ([colseq]
   (new-dataset {:name "unnamed"} colseq))
  ([] (new-dataset {:name "unnamed"} nil)))
