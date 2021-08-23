(ns testapp.webapp
  (:require [goog.dom :as gdom]
            [reagent.dom]
            [reagent.ratom :refer [atom]]
            [tech.v3.libs.cljs-ajax :refer [GET POST] :as tech-cljs-ajax]
            [tech.v3.dataset :as ds]
            [tech.v3.datatype.functional :as dfn]))


(defonce app* (atom {}))


(defn merge-by-key
  [lhs rhs colname]
  (->> (concat lhs rhs)
       (sort-by :time)
       (dedupe)))


(defn home
  []
  (GET "/data" {:handler #(swap! app* assoc
                                 :ds %
                                 :raw (mapv (partial into {}) (ds/rows %)))})
  (fn []
    [:div.container
     [:h1 "Hello from TMDJS"]
     [:div
      (if-let [ds (:ds @app*)]
        (let [raw (:raw @app*)]
          [:div
           [:div "Dataset"
            [:div [:pre (pr-str ds)]]]
           [:div "Some Timings"]
           [:table
            [:thead
             [:tr [:th "Operation"] [:th "Mapseq"] [:th "Dataset"]]]
            [:tbody
             [:tr [:td "Sort by time"]
              [:td (with-out-str (time (sort-by :time raw)))]
              [:td (with-out-str (time (ds/sort-by-column ds :time)))]]
             [:tr [:td "Merge by time"]
              [:td (with-out-str (time (count (merge-by-key raw raw :time))))]
              [:td (with-out-str (time (ds/row-count (ds/merge-by-column ds ds :time))))]]
             [:tr [:td "Serialize to transit"]
              [:td (with-out-str (time (ds/dataset->transit-str raw)))]
              [:td (with-out-str (time (ds/dataset->transit-str ds)))]]
             [:tr [:td "transit size (bytes)"]
              [:td (count (ds/dataset->transit-str raw))]
              [:td (count (ds/dataset->transit-str ds))]]
             [:tr [:td "Average temp"]
              [:td (with-out-str (time (dfn/mean (map :temp raw))))]
              [:td (with-out-str (time (dfn/mean (ds :temp))))]]
             [:tr [:td "Filter valid"]
              [:td (with-out-str (time (count (filter :valid? raw))))]
              [:td (with-out-str (time (ds/row-count (ds/filter-column ds :valid? identity))))]]
             [:tr [:td "Filter temps > 0.5"]
              [:td (with-out-str (time (count (filter #(> (:temp %) 0.5) raw))))]
              [:td (with-out-str (time (count (ds/filter-column ds :temp #(> % 0.5)))))]]
             ]]])
        [:div "Downloading dataset"])]]))


(defn render
  []
  (reagent.dom/render [home] (gdom/getElement "app")))

(defn ^:dev/after-load clear-cache-and-render!
  []
  (render))

(defn init
  "Entrypoint into the application."
  []
  (tech-cljs-ajax/add-java-time-handlers!)

  (render))
