(ns tech.v3.dataset.protocols)

(defprotocol PRowCount
  (-row-count [this]))

(defprotocol PColumnCount
  (-column-count [this]))

(defprotocol PMissing
  (-missing [this]))

(defprotocol PSelectRows
  (-select-rows [this rowidxs]))

(defprotocol PSelectColumns
  (-select-columns [this colnames]))

(defprotocol PColumn
  (-is-column? [col])
  (-column-buffer [col]))


(defprotocol PDataset
  ;;error on failure
  (-column [ds colname])
  ;;indexable object.
  (-rows [ds])
  (-rowvecs [ds])
  (-row-at [ds idx])
  (-rowvec-at [ds idx]))


(extend-type object
  PMissing
  (-missing [this] (js/Set.))
  PColumn
  (-is-column? [col] false)
  PRowCount
  (-row-count [this] (count this))
  PColumnCount
  (-column-count [this] 0))

(extend-type array
  PColumn
  (-is-column? [col] false))

(extend-type boolean
  PColumn
  (-is-column? [col] false))

(extend-type number
  PColumn
  (-is-column? [col] false))

(extend-type nil
  PColumn
  (-is-column? [col] false))
