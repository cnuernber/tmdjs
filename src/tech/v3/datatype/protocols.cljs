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
  (-add-all [this container]))
