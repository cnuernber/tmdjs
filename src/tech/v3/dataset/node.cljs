(ns tech.v3.dataset.node
  "Functions and helpers that require the node runtime."
  (:require [fs :as fs]
            [tech.v3.dataset :as ds]))


(defn transit-file->dataset
  "Given a file of transit data return a dataset."
  [fname]
  ;;returns buffer
  (-> (.readFileSync fs fname)
      ;;utf-8 encoded string
      (.toString)
      (ds/transit-str->dataset)))
