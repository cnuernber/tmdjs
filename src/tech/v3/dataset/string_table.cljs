(ns tech.v3.dataset.string-table
  "A dtype 'list' that unique-ifies its strings.  Unlike the jvm version, this
  simply uses an object array for the strings."
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.protocols :as dt-proto]))


(defn- intern-string
  [strval str-table]
  (if-let [retval (.get str-table strval)]
    retval
    (do (.set str-table strval strval)
        strval)))


(deftype StringTable [container str-table metadata]
  ICounted
  (-count [this] (count container))
  ICloneable
  (-clone [this]
    (clone container))
  IFn
  (-invoke [this idx] (nth this idx))
  IIndexed
  (-nth [this n] (nth container n))
  (-nth [this n not-found] (nth container n not-found))
  IMeta
  (-meta [this] metadata)
  IWithMeta
  (-with-meta [this metadata] (StringTable. container str-table metadata))
  IPrintWithWriter
  (-pr-writer [array writer opts]
    (-write writer (str "#string-table"
                        (take 20 (seq container)))))
  dt-proto/PElemwiseDatatype
  (-elemwise-datatype [this] :string)
  dt-proto/PSubBuffer
  (-sub-buffer [this off len]
    (StringTable. (dtype/sub-buffer container off len)
                  str-table
                  metadata))
  dt-proto/PListLike
  (-add [this val]
    (let [val (if val (intern-string val str-table) val)]
      (dt-proto/-add container val))
    this)
  (-add-all [this data]
    (dtype/iterate! #(dt-proto/-add this %) data)
    this)
  (-ensure-capacity [this buflen]
    (dt-proto/-ensure-capacity container buflen)))


(defn make-string-table
  [& [strdata]]
  (let [retval (StringTable. (dtype/make-list :string) (js/Map.) nil)]
    (when strdata (dt-proto/-add-all retval strdata))
    retval))
