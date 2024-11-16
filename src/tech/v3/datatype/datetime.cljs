(ns tech.v3.datatype.datetime
  "Minimal API for converting dealing with instants and local dates.  Please see
  juxt/tick for a more thorough treatment."
  (:require  [cljc.java-time.local-date :as local-date]
             [cljc.java-time.instant :as instant]
             [java.time :refer [LocalDate Instant]]
             [tech.v3.datatype.protocols :as dt-proto]))

(extend-protocol dt-proto/PDatatype
  LocalDate
  (-datatype [item] :local-date)
  Instant
  (-datatype [item] :instant))


(defn datetime-datatype?
  [dtype]
  (if (#{:local-date :instant} dtype)
    true
    false))


(defn local-date
  [] (local-date/now))


(defn local-date->epoch-days
  [ld]
  (local-date/to-epoch-day ld))


(defn epoch-days->local-date
  [ed]
  (local-date/of-epoch-day ed))


(defn instant
  [] (instant/now))


(defn epoch-milliseconds->instant
  [em] (instant/of-epoch-milli em))


(defn instant->epoch-milliseconds
  [in] (instant/to-epoch-milli in))


(defn epoch-microseconds->instant
  [em] (instant/of-epoch-milli (/ em 1000)))


(defn instant->epoch-microseconds
  [in] (* 1000 (instant/to-epoch-milli in)))
