(ns asynctopia.util
  (:require [clojure.core.async :as ca])
  (:import (clojure.core.async.impl.channels ManyToManyChannel)
           (java.util Collection)
           (clojure.lang IReduceInit)
           (java.time ZonedDateTime)))

(defn println-error-handler
  [^Throwable t]
  (println (str (ZonedDateTime/now)) \tab
           (.getName (Thread/currentThread)) \tab
           "[ERROR] -" (.getMessage t)))

(defonce throwable? (partial instance? Throwable))
(defonce reducible? (partial instance? IReduceInit))
(defonce chan?      (partial instance? ManyToManyChannel))

(defonce unit->ms
  {:microsecond 0.001
   :millisecond 1
   :second 1000
   :minute 60000
   :hour 3600000
   :day 86400000
   :month 2678400000})

(defn get-channel-buffer
  "Returns the underlying buffer of channel <ch>."
  [^ManyToManyChannel ch]
  (.buf ch))

(defn snapshot-java-collection
  [^Collection coll]
  (vec (.toArray coll)))
