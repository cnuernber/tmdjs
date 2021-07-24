(ns tech.v3.datatype.datetime
  "Minimal API for converting dealing with instants and local dates.  Please see
  juxt/tick for a more thorough treatment."
  (:require [java.time :refer [LocalDate Instant]]
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
  [] (.now LocalDate))


(defn local-date->epoch-days
  [^LocalDate ld]
  (.toEpochDay ld))


(defn epoch-days->local-date
  [ed]
  (.ofEpochDay LocalDate ed))


(defn instant
  [] (.now Instant))


(defn epoch-milliseconds->instant
  [em] (.ofEpochMilli Instant em))


(defn instant->epoch-milliseconds
  [^Instant in] (.toEpochMilli in))
