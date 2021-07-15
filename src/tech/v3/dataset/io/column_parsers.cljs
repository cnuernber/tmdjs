(ns tech.v3.dataset.io.column-parsers
  "A column parser is a parser that takes an indexed stream of values
  and returns a packed datastructure and a missing set.  nil values,
  #NaN values, and values of :tech.v3.dataset/missing are all interpreted as
  nil values."
  (:require [tech.v3.datatype :as dtype]
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
      (= val missing-val)))


(defn fixed-type-parser
  [colname dtype]
  (let [data (dt-col/make-container dtype)
        missing-val (dt-col/datatype->missing-value dtype)
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
      (-add-all [this data]
        (dtype/iterate! #(dt-proto/-add this %) data))
      PParser
      (-add-value! [this idx val]
        (add-missing! idx missing-val data missing)
        (dt-proto/-add this val))
      (-finalize [this rowcount]
        (add-missing! rowcount missing-val data missing)
        #:tech.v3.dataset{:data (or (dtype/as-agetable data) data)
                          :name colname
                          :force-datatype? true
                          :missing missing}))))


(deftype ObjParse [^:unsynchronized-mutable container
                   ^:unsynchronized-mutable container-dtype
                   ^:unsynchronized-mutable missing-val
                   missing
                   colname]
  ICounted
  (-count [this] (count container))
  dt-proto/PElemwiseDatatype
  (-elemwise-datatype [this] (dtype/elemwise-datatype container))
  dt-proto/PListLike
  (-add [this val]
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
                (dotimes [idx n-missing]
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
    (dtype/iterate! #(dt-proto/-add this %) data))
  PParser
  (-add-value! [this idx val]
    (add-missing! idx missing-val container missing)
    (dt-proto/-add this val))
  (-finalize [this rowcount]
    (add-missing! rowcount missing-val container missing)
    #:tech.v3.dataset{:data (or (dtype/as-agetable container) container)
                      :name colname
                      :force-datatype? true
                      :missing missing}))


(defn promotional-object-parser
  [colname]
  (ObjParse. (dt-col/make-container :boolean) :boolean false
             (js/Set.) colname))


(comment
  (do
    (let [p (fixed-type-parser :hey :boolean)]
      (-add-unindexed! p true)
      (-add-unindexed! p false)
      (-finalize p 4))

    (let [p (fixed-type-parser :hey :float32)]
      (-add-unindexed! p 20)
      (-add-unindexed! p 30)
      (-finalize p 4))

    (let [p (fixed-type-parser :hey :string)]
      (-add-unindexed! p "hey")
      (-add-unindexed! p "you")
      (-finalize p 4))

    (let [p (promotional-object-parser :hey)]
      (-add-unindexed! p nil)
      (-add-unindexed! p 5)
      (-add-unindexed! p 10)
      (-add-unindexed! p "data")
      (-finalize p 5))
    )
  )
