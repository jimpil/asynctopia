(ns asynctopia.kafka.producer
  (:require [asynctopia.channels :as channels]
            [asynctopia.ops :as ops]
            [clojure.core.async :as ca]
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
   :error!             ut/println-error-handler
   ;; not needed for idempotent producers
   ;:retries "0"
   ;:acks "all"
   })

(defn ->producer-record
  "Converts a map into a Kafka Producer Record.
  The map should contain [:topic :key :event]."
  [{:keys [topic key event]}]
  (org.apache.kafka.clients.producer.ProducerRecord. topic key event))


(defn create!
  "Creates a Kafka Producer client with a core.async interface given the broker's list and group id.
  After the Java Kafka producer is created it's saved in the `producers` atom with the following format:
  ```clojure
  {:uuid {:chan core-async-input-channel :producer java-kafka-producer}}
  ```
  A core.async process is created that reads from the input channel and sends the event to Java Kafka Producer.
  If a nil event is passed the process ends.
  This function returns the following map to the client
  ```clojure
  {:chan in-chan :id producer-id}
  ```
  "
  ([servers client-id]
   (create! servers client-id 1024 nil))
  ([servers client-id buf-or-n options]
   (let [^Map opts (-> {:bootstrap.servers servers
                        :client.id client-id}
                       (merge (dissoc defaults :error!) options)
                       walk/stringify-keys)
         producer (org.apache.kafka.clients.producer.KafkaProducer. opts)
         in-chan (channels/chan buf-or-n (map ->producer-record))]
     (ops/sink-with #(.send producer (pr-str %)) in-chan (:error! options identity))
     {:in-chan  in-chan
      :destroy! (fn []
                  (ca/close! in-chan)
                  (.close producer))})))


