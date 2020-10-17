(ns asynctopia.kafka.edn
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn])
  (:import (java.io PushbackReader ByteArrayOutputStream)))

(try
  (import [org.apache.kafka.common.serialization
           Serializer
           Deserializer])
  (catch Exception _
    (throw
      (IllegalStateException.
        "`kafka` dependency not found - aborting ..."))))

(deftype KeywordSerializer []
  org.apache.kafka.common.serialization.Serializer
  (configure [_ _ _])
  (serialize [_ _ kw]
    (.getBytes ^String (name kw)))
  (close [_]))

(deftype KeywordDeserializer []
  org.apache.kafka.common.serialization.Deserializer
  (configure [_ _ _])
  (deserialize [_ _ data]
    (keyword (String. ^bytes data "UTF-8")))
  (close [_]))

(deftype EdnSerializer []
  org.apache.kafka.common.serialization.Serializer
  (configure [_ _ _])
  (serialize [_ _ data]
    (with-open [out (ByteArrayOutputStream. 1024)
                wrt (io/writer out)]
      (binding [*print-length* nil
                *print-level*  nil
                *print-dup*    nil
                *print-meta*   nil]
        ;; bypass `pr` (leave *out* alone)
        (print-method data wrt))
      (.toByteArray out)))
  (close [_]))

(def READ-OPTS
  "Global options (`for edn/read`)."
  (atom {})) ;; (swap! READ-OPTS assoc :readers {...})

(deftype EdnDeserializer []
  org.apache.kafka.common.serialization.Deserializer
    (configure [_ _ _])
    (deserialize [_ _ data]
      (with-open [rdr (-> data io/reader PushbackReader.)]
        (edn/read @READ-OPTS rdr)))
  (close [_]))
