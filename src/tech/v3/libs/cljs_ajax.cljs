(ns tech.v3.libs.cljs-ajax
  "Bindings to use the dataset handlers in cljs GET/POST calls."
  (:require [ajax.core :as ajax]
            [ajax.interceptors :as ajax-interceptors]
            [ajax.transit :as ajax-transit]
            [tech.v3.dataset :as ds]
            [tech.v3.datatype.datetime :as dtype-dt]
            [java.time :refer [LocalDate Instant]]
            [cognitect.transit :as t]))


(defonce write-handlers* (atom (ds/transit-write-handler-map)))
(defonce read-handlers* (atom (ds/transit-read-handler-map)))


(defn add-transit-io-handlers!
  [datatype tag read-fn write-fn]
  (swap! write-handlers* assoc datatype (t/write-handler (constantly tag) write-fn))
  (swap! read-handlers* assoc tag read-fn))


(defn add-java-time-handlers!
  "Add handlers for java.time.LocalDate and java.time.Instant"
  []
  (add-transit-io-handlers! LocalDate "java.time.LocalDate"
                            dtype-dt/epoch-days->local-date
                            dtype-dt/local-date->epoch-days)
  (add-transit-io-handlers! Instant "java.time.Instant"
                            dtype-dt/epoch-milliseconds->instant
                            dtype-dt/instant->epoch-milliseconds))


(defn writer
  "Transit writer used for writing transit-json datasets and uses
`ds/transit-write-handler-map`"
  []
  (t/writer :json {:handlers @write-handlers*}))


(defn response-format
  "cljs-ajax interceptor that hardwires content-type to application/transit+json
  and uses `ds/transit-read-handler-map`."
  [& [content-type]]
  (ajax-interceptors/map->ResponseFormat
   {:content-type (or content-type ["application/transit+json"])
    :description "Transit response"
    :read (ajax-transit/transit-read-fn {:handlers @read-handlers*})}))


(defn opt-map
  "Options map that must be included in the cljs-ajax request in order
  to activate dataset->transit pathways."
  []
  {:writer (writer) :response-format (response-format)})


(defn transit-request
  "Perform a cljs-ajax request using the select cljs-ajax.core method and merging
  [[opt-map]] first into the options."
  [method url options]
  (method url (merge (opt-map) options)))


(defn GET
  "Drop in replacement for cljs-ajax.core/GET"
  [url options] (transit-request ajax/GET url options))


(defn POST
  "Drop in replacement for cljs-ajax.core/POST"
  [url options] (transit-request ajax/POST url options))
