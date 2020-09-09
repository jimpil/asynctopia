(ns asynctopia.util)

(defn println-error-handler
  [^Throwable t]
  (println "[ERROR]:" (.getMessage t)))

(defonce throwable? (partial instance? Throwable))
(defonce noop       (constantly nil))

(defonce unit->ms
  {:microsecond 0.001
   :millisecond 1
   :second 1000
   :minute 60000
   :hour 3600000
   :day 86400000
   :month 2678400000})
