(ns tech.v3.dataset.io.column-parsers
  "A column parser is a parser that takes an indexed stream of values
  and returns a packed datastructure and a missing set.  nil values,
  #NaN values, and values of :tech.v3.dataset/missing are all interpreted as
  nil values."
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.protocols :as dt-proto]
            [tech.v3.dataset.impl.column :as dt-col]))


(defprotocol PParser
  (-add-value! [this idx val])
  (-finalize [this rowcount]))


(defn- add-missing!
  [rowcount missing-val data missing]
  (let [cur-count (count data)
        n-prev (- rowcount cur-count)]
    ;;add all missing values so far
    (dotimes [idx n-prev]
      (.add missing (+ idx cur-count))
      (dt-proto/-add data missing-val))))


(defn- missing?
  [val missing-val]
  (or (nil? val)
      (= val :tech.v3.dataset/missing)
      (and (= val missing-val)
           (not (boolean? val)))))


(defn fixed-type-parser
  [colname dtype]
  (let [data (dt-col/make-container dtype)
        missing-val (if (casting/numeric-type? dtype)
                      ##NaN
                      (dt-col/datatype->missing-value dtype))
        missing (js/Set.)]
    (reify
      ICounted
      (-count [this] (count data))
      dt-proto/PElemwiseDatatype
      (-elemwise-datatype [this] dtype)
      dt-proto/PListLike
      (-add [this val]
        (if (missing? val missing-val)
          (do
            (.add missing (count data))
            (dt-proto/-add data missing-val))
          (dt-proto/-add data val)))
      (-add-all [this new-data]
        (if-let [aget-data (dtype/as-datatype-accurate-agetable new-data)]
          ;;Do scanning/adding of data separate from checking for missing
          (let [cur-off (count data)]
            (dt-proto/-add-all data aget-data)
            (dotimes [idx (count aget-data)]
              (let [dval (aget aget-data idx)]
                (when (missing? dval missing-val)
                  (.add missing (+ idx cur-off) dval)))))
          ;;if we have absolutely no idea what this is.
          (dtype/iterate! #(dt-proto/-add this %) new-data)))
      PParser
      (-add-value! [this idx val]
        (add-missing! idx missing-val data missing)
        (dt-proto/-add this val))
      (-finalize [this rowcount]
        (add-missing! rowcount missing-val data missing)
        ;;We pay a performance penalty in order to have correct datatypes
        ;;for array-based objects
        #:tech.v3.dataset{:data (or (dtype/as-datatype-accurate-agetable data) data)
                          :name colname
                          :force-datatype? true
                          :missing missing}))))


(deftype ObjParse [^:unsynchronized-mutable container
                   ^:unsynchronized-mutable container-dtype
                   ^:unsynchronized-mutable missing-val
                   missing
                   colname]
  ICounted
  (-count [_this] (count container))
  dt-proto/PElemwiseDatatype
  (-elemwise-datatype [_this] (dtype/elemwise-datatype container))
  dt-proto/PListLike
  (-add [_this val]
    (if (missing? val missing-val)
      (do
        (.add missing (count container))
        (dt-proto/-add container missing-val))
      (let [val-dt (dtype/datatype val)]
        (if (or (= container-dtype :object)
                (= val-dt container-dtype))
          (dt-proto/-add container val)
          ;;Container has no meaningful data
          (let [n-missing (count missing)]
            (if (>= n-missing (count container))
              (let [new-container (dt-col/make-container val-dt)
                    new-missing (dt-col/datatype->missing-value val-dt)]
                (dotimes [_idx n-missing]
                  (dt-proto/-add new-container new-missing))
                (set! container new-container)
                (set! container-dtype val-dt)
                (set! missing-val new-missing))
              ;;Else container has meaningful data- promote to object
              (let [new-container (dt-col/make-container :object)
                    new-missing nil]
                (dotimes [idx (count container)]
                  (if (.has missing idx)
                    (dt-proto/-add new-container new-missing)
                    (dt-proto/-add new-container (nth container idx))))
                (set! container new-container)
                (set! container-dtype :object)
                (set! missing-val new-missing)))
            (dt-proto/-add container val))))))
  (-add-all [this data]
    (let [agetable-data (dtype/as-datatype-accurate-agetable data)]
      ;;Potentially we can just straight line this data.  This is really purely a fastpath
      ;;for numeric data
      (if (and agetable-data
               (or (>= (count missing) (count container))
                   (= container-dtype (dtype/elemwise-datatype agetable-data))))
        (do
          ;;when we have to change our container datatype
          (when (not= container-dtype (dtype/elemwise-datatype agetable-data))
            (let [data-dtype (dtype/elemwise-datatype agetable-data)
                  new-container (dt-col/make-container data-dtype)
                  new-missing (dt-col/datatype->missing-value data-dtype)
                  n-missing (count missing)]
              (dotimes [_idx n-missing]
                (dt-proto/-add new-container new-missing))
              (set! container new-container)
              (set! container-dtype data-dtype)
              (set! missing-val new-missing)))
          (let [cur-off (count container)]
            (dt-proto/-add-all container agetable-data)
            ;;scan agetable data for missing
            (when (#{:float32 :float64 :object} (dtype/elemwise-datatype agetable-data))
              (dotimes [idx (count agetable-data)]
                (let [dval (aget agetable-data idx)]
                  (when (or (nil? dval)
                            (js/isNaN dval))
                    ;;record NaN
                    (.add missing (+ idx cur-off))))))))
        ;;fallback to basic iteration
        (dtype/iterate! #(dt-proto/-add this %) data))))
  PParser
  (-add-value! [this idx val]
    (add-missing! idx missing-val container missing)
    (dt-proto/-add this val))
  (-finalize [_this rowcount]
    (add-missing! rowcount missing-val container missing)
    #:tech.v3.dataset{:data (or (dtype/as-datatype-accurate-agetable container) container)
                      :name colname
                      :force-datatype? true
                      :missing missing}))


(defn promotional-object-parser
  [colname]
  (ObjParse. (dt-col/make-container :boolean) :boolean false
             (js/Set.) colname))
