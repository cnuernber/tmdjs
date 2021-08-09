(ns tech.v3.libs.cljs-ajax
  "Bindings to use the dataset handlers in cljs GET/POST calls."
  (:require [ajax.core :as ajax]
            [ajax.interceptors :as ajax-interceptors]
            [ajax.transit :as ajax-transit]
            [tech.v3.dataset :as ds]
            [cognitect.transit :as transit]))


(def ^{:doc "Transit writer used for writing transit-json datasets and uses
`ds/transit-write-handler-map`" }
  writer (transit/writer :json {:handlers (ds/transit-write-handler-map)}))


(def ^{:doc "cljs-ajax interceptor that hardwires content-type to application/transit+json
and uses `ds/transit-read-handler-map`." }
  response-format
  (ajax-interceptors/map->ResponseFormat
   {:content-type ["application/transit+json"]
    :description "Transit response"
    :read (ajax-transit/transit-read-fn {:handlers (ds/transit-read-handler-map)})}))


(def ^{:doc "Options map that must be included in the cljs-ajax request in order
to activate dataset->transit pathways."} opt-map
  {:writer writer :response-format response-format})


(defn transit-request
  "Perform a cljs-ajax request using the select cljs-ajax.core method and merging
  [[opt-map]] first into the options."
  [method url options]
  (method url (merge opt-map options)))


(defn GET
  "Drop in replacement for cljs-ajax.core/GET"
  [url options] (transit-request ajax/GET url options))


(defn POST
  "Drop in replacement for cljs-ajax.core/POST"
  [url options] (transit-request ajax/POST url options))
