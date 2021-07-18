(ns tech.v3.datatype.protocols)


(defprotocol PDatatype
  (-datatype [item]))


(extend-protocol PDatatype
  object
  (-datatype [item] :object)
  Keyword
  (-datatype [item] :keyword)
  string
  (-datatype [item] :string)
  boolean
  (-datatype [item] :boolean)
  number
  (-datatype [item] :float64)
  PersistentArrayMap
  (-datatype [item] :persistent-map)
  PersistentHashMap
  (-datatype [item] :persistent-map)
  js/Date
  (-datatype [item] :instant))


(defprotocol PElemwiseDatatype
  (-elemwise-datatype [item]))


(extend-protocol PElemwiseDatatype
  object
  (-elemwise-datatype [item] (-datatype item))
  string
  (-elemwise-datatype [item] (-datatype item))
  boolean
  (-elemwise-datatype [item] (-datatype item))
  number
  (-elemwise-datatype [item] (-datatype item)))


(extend-type array
  PElemwiseDatatype
  (-elemwise-datatype [item] :object)
  PDatatype
  (-datatype [item] :array))


(extend-type array
  PDatatype
  (-datatype [item] :object))


(defprotocol PSubBufferCopy
  (-sub-buffer-copy [item offset length]))

(defprotocol PSubBuffer
  (-sub-buffer [item offset length]))


(defprotocol PToTypedArray
  (-convertible-to-typed-array? [buf])
  (->typed-array [buf]))


(extend-type object
  PToTypedArray
  (-convertible-to-typed-array? [buf] false))

(extend-type array
  PToTypedArray
  (-convertible-to-typed-array? [buf] false))


(defprotocol PToJSArray
  (-convertible-to-js-array? [buf])
  (->js-array [buf]))


(extend-type object
  PToJSArray
  (-convertible-to-js-array? [buf] false))


(defprotocol PAgetable
  (-convertible-to-agetable? [buf])
  (->agetable [buf]))


(extend-protocol PAgetable
  object
  (-convertible-to-agetable? [buf] (or (-convertible-to-js-array? buf)
                                       (-convertible-to-typed-array? buf)))
  (->agetable [buf] (cond
                      (-convertible-to-js-array? buf) (->js-array buf)
                      (-convertible-to-typed-array? buf) (->typed-array buf)
                      :else
                      (throw (js/Error. "Object is not aget-able"))))
  array
  (-convertible-to-agetable? [buf] true)
  (->agetable [buf] buf))


(defprotocol PSetValue
  (-set-value! [item idx data]))


(defprotocol PSetConstant
  (-set-constant! [item offset elem-count data]))


(defprotocol PListLike
  (-add [this elem])
  (-add-all [this container])
  (-ensure-capacity [this capacity]))


(defprotocol PBitmapSet
  (-set-or [lhs rhs])
  (-set-and [lhs rhs])
  (-set-and-not [lhs rhs])
  (-set-offset [this off]))
