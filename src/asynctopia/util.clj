(ns asynctopia.util
  (:import (clojure.core.async.impl.channels ManyToManyChannel)
           (clojure.lang IReduceInit)
           (java.time ZonedDateTime)
           (java.util UUID)))

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

(defn snapshot-java-collection
  [coll]
  (seq (to-array coll)))

(defn uuid-str []
  (str (UUID/randomUUID)))

(defn stringify-keys-1 [m]
  (into {} (map (fn [[k v]] [(name k) v])) m))