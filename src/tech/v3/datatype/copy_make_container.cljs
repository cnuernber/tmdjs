(ns tech.v3.datatype.copy-make-container
  (:require [tech.v3.datatype.arrays :as dt-arrays]
            [tech.v3.datatype.base :as dt-base]))


(defn make-container
  [dtype len-or-data]
  (let [data (if (number? len-or-data)
               nil
               (if-let [ag-data (dt-base/as-typed-array len-or-data)]
                 ag-data
                 (if-let [ag-data (dt-base/as-js-array len-or-data)]
                   ag-data
                   (if (and (dt-base/counted? len-or-data)
                            (dt-base/indexed? len-or-data))
                     len-or-data
                     (vec len-or-data)))))
        dlen (if (number? len-or-data)
               len-or-data
               (count data))
        container (dt-arrays/make-array dtype dlen)]
    (when data (dt-base/set-value! container 0 data))
    container))
