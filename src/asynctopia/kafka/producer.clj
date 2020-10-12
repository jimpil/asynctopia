(ns asynctopia.kafka.producer
  (:require [asynctopia.channels :as channels]
            [asynctopia.ops :as ops]
            [asynctopia.util :as ut]
            [clojure.walk :as walk])
  (:import (java.util Map)))

(try
  (import org.apache.kafka.clients.producer.KafkaProducer)
  (import org.apache.kafka.clients.producer.ProducerRecord)
  (catch Exception _
    (throw
      (IllegalStateException.
        "`kafka` dependency not found - aborting ..."))))

(def defaults
  {:batch.size         "1024"
   :linger.ms          "1"
   :max.block.ms       "1"
   :buffer.memory      "5242880" ;; 5MB
   :key.serializer     "org.apache.kafka.common.serialization.StringSerializer"
   :value.serializer   "org.apache.kafka.common.serialization.StringSerializer"
   :enable.idempotence true ;; 'exactly-once' delivery semantics
   ;; not needed for idempotent producers
   ;:retries "0"
   ;:acks "all"
   })

(defn ->producer-record
  "Converts a map into a Kafka Producer Record.
  The map should contain [:topic :key :event]."
  [{:keys [topic key event]}]
  (org.apache.kafka.clients.producer.ProducerRecord. topic key (pr-str event)))

(defn edn-producer
  "Creates a Kafka Producer, an input-channel (per buf-or-n/error!),
   and sets up a `go-loop` forwarding values (maps containing :topic :key :event)
   from the input-channel on to the Producer. Closing the input-channel exits the loop,
   and closes the Kafka Producer. Returns the aforementioned input-channel."
  ([servers client-id]
   (edn-producer servers client-id 1024 nil))
  ([servers client-id buf-or-n options]
   (let [^Map opts (-> {:bootstrap.servers servers
                        :client.id client-id}
                       (merge defaults options)
                       walk/stringify-keys)
         error!   (get opts "error!" ut/println-error-handler)
         producer (org.apache.kafka.clients.producer.KafkaProducer. opts)
         in-chan  (channels/chan buf-or-n (map ->producer-record) error!)]
     ;; start the go-loop and wait/park for inputs
     (ops/sink-with #(.send producer %)
                    in-chan
                    error!
                    #(.close producer))
     in-chan)))

