(ns asynctopia.kafka.producer
  (:require [asynctopia.channels :as channels]
            [asynctopia.ops :as ops]
            [asynctopia.util :as ut]
            [clojure.string :as str])
  (:import (java.util Map)
           (org.apache.kafka.common.header Header)))

(try
  (import [org.apache.kafka.clients.producer
           KafkaProducer
           ProducerRecord])
  (require 'asynctopia.kafka.edn)
  (catch Exception _
    (throw
      (IllegalStateException.
        "`kafka` dependency not found - aborting ..."))))

(def defaults
  {:batch.size         "1024"
   :linger.ms          "1"
   :max.block.ms       "1"
   :buffer.memory      "5242880" ;; 5MB
   :key.serializer     "asynctopia.kafka.edn.EdnSerializer"
   :value.serializer   "asynctopia.kafka.edn.EdnSerializer"
   :enable.idempotence true ;; 'exactly-once' delivery semantics
   ;; not needed for idempotent producers
   ;:retries "0"
   ;:acks "all"
   })

(defn kheader
  [[^String k ^String v]]
  (reify Header
    (key [_] k)
    (value [_] (.getBytes v))))

(defn ->producer-record
  "Converts a map into a Kafka Producer Record.
  The map should contain [:topic :key :event]."
  [{:keys [^String topic key event headers] :as m}]
  (let [kheaders (some->> (or headers (meta m)) (map kheader))]
    (org.apache.kafka.clients.producer.ProducerRecord.
    topic nil key event ^Iterable kheaders)))

(defn edn-producer
  "Creates a Kafka Producer, an input-channel (per buf-or-n/error!),
   and sets up a `go-loop` forwarding values (maps containing :topic :key :event)
   from the input-channel on to the Producer. Closing the input-channel exits the loop,
   and closes the Kafka Producer. Returns the aforementioned input-channel."
  ([]
   (edn-producer "localhost:9092" "local-producer"))
  ([servers client-id]
   (edn-producer servers client-id 1024 nil))
  ([servers client-id buf-or-n]
   (edn-producer servers client-id buf-or-n nil))
  ([servers client-id buf-or-n xform]
   (edn-producer servers client-id buf-or-n xform nil))
  ([servers client-id buf-or-n xform options]
   (let [servers-str (if (string? servers) servers (str/join \, servers))
         ^Map opts (-> {:bootstrap.servers servers-str
                        :client.id         client-id}
                       (merge defaults options)
                       ut/stringify-keys-1)
         error!   (get opts "error!" ut/println-error-handler)
         producer (org.apache.kafka.clients.producer.KafkaProducer. opts)
         in-chan  (channels/chan buf-or-n
                                 (cond->> (map ->producer-record)
                                          xform (comp xform))
                                 error!)]
     ;; start the go-loop and wait/park for inputs
     (ops/sink-with! #(.send producer %)
                     in-chan
                     error!
                     #(future (.close producer))) ;; don't block here
     in-chan)))

