(ns tech.v3.libs.transit
  "Transit bindings for the jvm version of tech.v3.dataset."
  (:require [tech.v3.dataset :as ds]
            [tech.v3.dataset.impl.dataset :as ds-impl]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.datatype.bitmap :as bitmap]
            [tech.v3.datatype.nio-buffer :as nio-buffer]
            [primitive-math :as pmath]
            [cognitect.transit :as t])
  (:import [tech.v3.dataset.impl.dataset Dataset]
           [tech.v3.dataset.impl.column Column]
           [java.nio ByteBuffer]
           [java.util Base64 HashMap]))

(set! *warn-on-reflection* true)


(defn numeric-data->b64
  [^Column col]
  (let [src-data (dtype/->reader col)
        n-elems (count col)
        ^ByteBuffer bbuf
        (case (casting/host-flatten (dtype/elemwise-datatype col))
          :int8
          (let [retval (ByteBuffer/wrap (byte-array n-elems))
                dst-data (dtype/->buffer retval)]
            (dotimes [idx (count col)]
              (.writeByte dst-data idx (unchecked-byte (.readLong src-data idx))))
            retval)
          :int16
          (let [retval (ByteBuffer/wrap (byte-array (* Short/BYTES n-elems)))
                dst-data (.asShortBuffer retval)]
            (dotimes [idx (count col)]
              (.put dst-data idx (unchecked-short (.readLong src-data idx))))
            retval)
          :int32
          (let [retval (ByteBuffer/wrap (byte-array (* Integer/BYTES n-elems)))
                dst-data (.asIntBuffer retval)]
            (dotimes [idx (count col)]
              (.put dst-data idx (unchecked-int (.readLong src-data idx))))
            retval)
          :int64
          (let [retval (ByteBuffer/wrap (byte-array (* Long/BYTES n-elems)))
                dst-data (.asLongBuffer retval)]
            (dotimes [idx (count col)]
              (.put dst-data idx (.readLong src-data idx)))
            retval)
          :float32
          (let [retval (ByteBuffer/wrap (byte-array (* Float/BYTES n-elems)))
                dst-data (.asFloatBuffer retval)]
            (dotimes [idx (count col)]
              (.put dst-data idx (.readDouble src-data idx)))
            retval)
          :float64
          (let [retval (ByteBuffer/wrap (byte-array (* Double/BYTES n-elems)))
                dst-data (.asDoubleBuffer retval)]
            (dotimes [idx (count col)]
              (.put dst-data idx (.readDouble src-data idx)))
            retval))]
    (String. (.encode (Base64/getEncoder) (.array bbuf)))))


(defn string-col->data
  [col]
  (let [strdata (dtype/make-list :string)
        indexes (dtype/make-list :int32)
        seen (HashMap.)]
    (dotimes [idx (count col)]
      (let [strval (or (col idx) "")
            idx (.computeIfAbsent seen strval (reify java.util.function.Function
                                                (apply [this arg]
                                                  (let [retval (count strdata)]
                                                    (.add strdata strval)
                                                    retval))))]
        (.add indexes idx)))
    {:strtable (vec strdata)
     :indexes (numeric-data->b64 indexes)}))


(defn obj-col->numeric-b64
  [col dst-dtype convert-fn]
  (let [data (dtype/make-list dst-dtype)]
    (dotimes [idx (count col)]
      (.add data (convert-fn (or (col idx) 0))))
    (numeric-data->b64 data)))


(defn- col->data
  [col]
  (let [col-dt (packing/unpack-datatype (dtype/elemwise-datatype col))]
    {:metadata (assoc (meta col) :datatype col-dt)
     :missing (vec (bitmap/bitmap->efficient-random-access-reader
                    (ds/missing col)))
     :data
     (cond
       (casting/numeric-type? col-dt)
       (numeric-data->b64 col)
       (= :boolean col-dt)
       (numeric-data->b64 (dtype/make-reader  :uint8 (count col)
                                             (if (col idx) 1 0)))
       (= :string col-dt)
       (string-col->data col)
       (#{:packed-local-date :local-date} col-dt)
       (obj-col->numeric-b64 col :int32 dtype-dt/local-date->days-since-epoch)
       (#{:packed-instant :instant} col-dt)
       (obj-col->numeric-b64 col :int64 dtype-dt/instant->milliseconds-since-epoch)
       :else ;;Punt!!
       (vec col))}))


(defn dataset->data
  "Dataset to transit-safe data"
  [ds]
  {:metadata (meta ds)
   :flavor :transit
   :version 1
   :columns (mapv col->data (ds/columns ds))})


(defn b64->numeric-data
  [^String data dtype]
  (let [byte-data (.decode (Base64/getDecoder) (.getBytes data))
        bbuf (ByteBuffer/wrap byte-data)]
    (case dtype
      :int8 (dtype/->buffer byte-data)
      :uint8 (dtype/make-reader :uint8 (alength byte-data)
                                (pmath/byte->ubyte (aget byte-data idx)))
      :int16 (let [sbuf (.asShortBuffer bbuf)]
               (dtype/make-reader :int16 (.limit sbuf) (.get sbuf idx)))
      :uint16 (let [sbuf (.asShortBuffer bbuf)]
                (dtype/make-reader :uint16 (.limit sbuf) (pmath/short->ushort
                                                          (.get sbuf idx))))
      :int32 (let [ibuf (.asIntBuffer bbuf)]
               (dtype/make-reader :int32 (.limit ibuf) (.get ibuf idx)))
      :uint32 (let [ibuf (.asIntBuffer bbuf)]
                (dtype/make-reader :uint32 (.limit ibuf) (pmath/int->uint (.get ibuf idx))))
      :int64 (let [lbuf (.asLongBuffer bbuf)]
               (dtype/make-reader :int64 (.limit lbuf) (.get lbuf idx)))
      :uint64 (let [lbuf (.asLongBuffer bbuf)]
                (dtype/make-reader :uint64 (.limit lbuf) (.get lbuf idx)))
      :float32 (let [fbuf (.asFloatBuffer bbuf)]
                 (dtype/make-reader :float32 (.limit fbuf) (.get fbuf idx)))
      :float64 (let [fbuf (.asDoubleBuffer bbuf)]
                 (dtype/make-reader :float64 (.limit fbuf) (.get fbuf idx))))))


(defn str-data->coldata
  [{:keys [strtable indexes]}]
  (let [indexes (dtype/->reader (b64->numeric-data indexes :int32))
        strdata (dtype/->reader strtable)]
    (dtype/make-reader :string (count indexes) (strdata (indexes idx)))))


(defn data->dataset
  [{:keys [metadata columns] :as ds-data}]
  (errors/when-not-errorf
   (and metadata columns)
   "Passed in data does not appear to have metadata or columns")
  (->> (:columns ds-data)
       (map
        (fn [{:keys [metadata missing data]}]
          (let [dtype (:datatype metadata)]
            #:tech.v3.dataset{:metadata metadata
                              :missing (bitmap/->bitmap missing)
                              ;;do not re-scan data.
                              :force-datatype? true
                              :data
                              (cond
                                (casting/numeric-type? dtype)
                                (b64->numeric-data data dtype)
                                (= :boolean dtype)
                                (let [ibuf (b64->numeric-data data :int8)]
                                  (dtype/make-reader :boolean (count ibuf)
                                                     (if (== 0 (unchecked-long (ibuf idx)))
                                                       false true)))
                                (= :string dtype)
                                (str-data->coldata data)
                                (= :local-date dtype)
                                (->> (b64->numeric-data data :int32)
                                     (dtype/emap dtype-dt/days-since-epoch->local-date
                                                 :local-date))
                                (= :instant dtype)
                                (->> (b64->numeric-data data :int64)
                                     (dtype/emap dtype-dt/milliseconds-since-epoch->instant
                                                 :instant))
                                :else
                                (dtype/make-container dtype data))
                              :name (:name metadata)})))
       (ds-impl/new-dataset (:metadata ds-data))))

(def write-handlers {Dataset (t/write-handler "tech.v3.dataset" dataset->data)})
(def read-handlers {"tech.v3.dataset" (t/read-handler data->dataset)})

(defn dataset->transit
  "Convert a dataset into transit"
  [ds out & [format handlers]]
  (let [writer (t/writer out (or format :json) {:handlers (merge write-handlers handlers)})]
    (t/write writer ds)))


(defn dataset->transit-str
  [ds & [format handlers]]
  (let [out (java.io.ByteArrayOutputStream.)]
    (dataset->transit ds out format handlers)
    (String. (.toByteArray out))))


(defn transit->dataset
  [in & [format handlers]]
  (let [reader (t/reader in (or format :json) {:handlers (merge read-handlers handlers)})]
    (t/read reader)))

(comment
  (defn master-ds
    []
    (ds/->dataset {:a (mapv double (range 5))
                   :b (repeat 5 :a)
                   :c (repeat 5 "hey")
                   :d (repeat 5 {:a 1 :b 2})
                   :e (repeat 4 [1 2 3])
                   :f (repeat 5 (dtype-dt/local-date))
                   :g (repeat 5 (dtype-dt/instant))
                   :h [true false true true false]}))
  )
