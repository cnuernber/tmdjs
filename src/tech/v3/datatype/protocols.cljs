(ns tech.v3.datatype.protocols)


(defprotocol PElemwiseDatatype
  (-elemwise-datatype [item]))


(extend-type object
  PElemwiseDatatype
  (-elemwise-datatype [item] :object))

(extend-type array
  PElemwiseDatatype
  (-elemwise-datatype [item] :object))


(defprotocol PDatatype
  (-datatype [item]))


(extend-type object
  PDatatype
  (-datatype [item] :object))

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


(defprotocol PSetValue
  (-set-value! [item idx data]))


(defprotocol PSetConstant
  (-set-constant! [item offset elem-count data]))
