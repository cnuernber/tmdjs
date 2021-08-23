(ns testapp.main
  (:require [org.httpkit.server :as server]
            [hiccup.page :as hiccup]
            [ring.util.response :as response]
            [ring.middleware.resource :refer [wrap-resource]]
            [ring.middleware.content-type :refer [wrap-content-type]]
            ;;automatic handling of transit->dataset conversion
            [tech.v3.libs.muuntaja :refer [wrap-format-java-time]]
            [ring.middleware.cookies :refer [wrap-cookies]]
            [ring.middleware.gzip :refer [wrap-gzip]]
            [bidi.ring :as bidi-ring]
            [clojure.tools.logging :as log]
            [tech.v3.dataset :as ds]
            [tech.v3.libs.transit :as tech-transit])
  (:import [java.time LocalDate Instant])
  (:gen-class))


(defn- home-page
  [request]
  (-> (hiccup/html5
       {:lang "en"}
       [:head
        [:meta {:charset "utf-8"}]
        [:title "TMD For CLJS"]
        [:meta {:name "viewport" :content "width=device-width, initial-scale=1"}]
        [:meta {:name "description" :content "Dataframes for cljs"}]
        [:link {:href "//fonts.googleapis.com/css?family=Raleway:400,300,600"
                :rel "stylesheet"
                :type "text/css"}]
        [:link {:rel "stylesheet" :href "css/normalize.css"}]
        [:link {:rel "stylesheet" :href "css/skeleton.css"}]
        [:body [:div#app]]
        [:script {:src "js/app.js" :type "text/javascript"}]])
      (response/response)
      (response/header "Content-Type" "text/html")))


(defn generate-data
  [request]
  (-> (ds/->dataset (repeatedly 10000 #(hash-map :time (rand)
                                                 :temp (rand)
                                                 :temp1 (rand)
                                                 :temp2 (rand)
                                                 :valid? (if (> (rand) 0.5)
                                                           true
                                                           false))))
      (response/response)))


(defn datetest
  [request]
  (response/response {:local-date (LocalDate/now)
                      :instant (Instant/now)}))


(defn stocks-data
  [request]
  (-> (ds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv" {:key-fn keyword})
      (response/response)))


(def routes ["/" {:get [["data" #'generate-data]
                        ["stocks" #'stocks-data]
                        ["datetest" #'datetest]
                        [true #'home-page]]}])

(defn handler
  []
  (-> (bidi-ring/make-handler routes)
      (wrap-format-java-time {:write-handlers tech-transit/java-time-write-handlers
                              :read-handlers tech-transit/java-time-read-handlers})
      (wrap-cookies)
      (wrap-resource "public")
      (wrap-content-type)
      (wrap-gzip)))


(defonce ^:private server* (atom nil))


(defn start-server
  ([{:keys [port]
     :or {port 3000}
     :as options}]
   (swap! server*
          (fn [existing-server]
            (if existing-server
              (do
                (log/infof "Restarting server on port %d" port)
                (existing-server))
              (log/infof "Starting server on port %d" port))
            (server/run-server (handler)
                               (merge {:port port}
                                      options)))))
  ([]
   (start-server nil)))


(defn -main
  [& args]
  (start-server)
  (log/infof "Main function exiting-server still running"))
