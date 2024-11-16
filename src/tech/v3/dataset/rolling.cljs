(ns tech.v3.dataset.rolling
  (:require [tech.v3.dataset :as ds]
            [tech.v3.datatype :as dt]))


(defn mean
  [data]
  (let [data (dt/as-agetable data)
        n (dt/ecount data)]
    (loop [idx 0
           sum 0.0]
      (if (< idx n)
        (recur (unchecked-inc idx) (+ sum (aget data idx)))
        (/ sum n)))))

(defn scalar-fixed-rolling
  [ds cname filter-width reducer]
  (let [ne (ds/row-count ds)
        col (ds cname)
        data (or (dt/as-agetable col) (dt/as-agetable (dt/make-container :float32 col)))
        rv (dt/make-container :float32 ne)
        rvd (dt/as-agetable rv)
        filter-width (long filter-width)]
    (dotimes [idx ne]
      (let [sidx (max 0 (- idx filter-width))
            eidx (min ne (+ idx filter-width))
            sbuf (dt/sub-buffer data sidx (- eidx sidx))]
        (aset rvd idx (reducer sbuf))))
    rv))
