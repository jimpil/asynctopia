(ns asynctopia.kafka.producer
  (:require [asynctopia.channels :as channels]
            [asynctopia.ops :as ops]
            [asynctopia.util :as ut]
            [clojure.string :as str])
  (:import (java.util Map)))

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

(defn ->producer-record
  "Converts a map into a Kafka Producer Record.
  The map should contain [:topic :key :event]."
  [{:keys [topic key event]}]
  (org.apache.kafka.clients.producer.ProducerRecord. topic key event))

(defn edn-producer
  "Creates a Kafka Producer, an input-channel (per buf-or-n/error!),
   and sets up a `go-loop` forwarding values (maps containing :topic :key :event)
   from the input-channel on to the Producer. Closing the input-channel exits the loop,
   and closes the Kafka Producer. Returns the aforementioned input-channel."
  ([]
   (edn-producer "localhost:9092" "local-producer"))
  ([servers client-id]
   (edn-producer servers client-id 1024 nil))
  ([servers client-id buf-or-n options]
   (let [servers-str (if (string? servers) servers (str/join \, servers))
         ^Map opts (-> {:bootstrap.servers servers-str
                        :client.id         client-id}
                       (merge defaults options)
                       ut/stringify-keys-1)
         error!   (get opts "error!" ut/println-error-handler)
         producer (org.apache.kafka.clients.producer.KafkaProducer. opts)
         in-chan  (channels/chan buf-or-n (map ->producer-record) error!)]
     ;; start the go-loop and wait/park for inputs
     (ops/sink-with! #(.send producer %)
                     in-chan
                     error!
                     #(future (.close producer))) ;; don't block here
     in-chan)))

