(ns tech.v3.libs.muuntaja
  "Bindings to allow you to send/receive datasets via transit-json using
  server-side muuntaja bindings.  For client side bindings, please see
  tech.v3.libs.cljs-ajax."
  (:require [muuntaja.core :as m]
            [tech.v3.libs.transit :as tech-transit]
            [muuntaja.middleware :as muuntaja-middle]))


;;https://github.com/metosin/muuntaja/blob/master/doc/With-Ring.md


(defn- muuntaja-opts
  [{:keys [read-handlers write-handlers]}]
  (reduce (fn [opts opt-key]
            (update-in opts
                       [:formats opt-key]
                       merge
                       {:encoder-opts {:handlers (merge tech-transit/write-handlers
                                                        write-handlers)}
                        :decoder-opts {:handlers (merge tech-transit/read-handlers
                                                        read-handlers)}}))
          m/default-options
          ["application/transit+json" "application/transit+msgpack"]))


(defn- muuntaja
  [& [options]]
  (m/create (muuntaja-opts options)))


(defn wrap-format
  "Wrapper for muuntaja wrap-format that adds in the transit read/write handlers
  for datasets.

  Options

  * `:write-handlers` - transit handler-map of extra handlers to use when writing data.
  * `:read-handlers` - transit handler-map of extra handlers to use when reading data."
  [handler & [options]]
  (muuntaja-middle/wrap-format handler (muuntaja options)))


(defn wrap-format-java-time
  "Wrap-format support datasets and scalar java.time.LocalDate and java.time.Instant objects."
  [handler & [options]]
  (wrap-format handler (assoc options
                              :write-handlers tech-transit/java-time-write-handlers
                              :read-handlers tech-transit/java-time-read-handlers)))
