(ns asynctopia.kafka.consumer
  (:require [clojure.core.async :as ca]
            [asynctopia.channels :as channels]
            [asynctopia.core :as c]
            [clojure.string :as str]
            [asynctopia.util :as ut]
            [clojure.core.protocols :as pclj]
            [clojure.datafy :as df])
  (:import (java.util Map ArrayList Collection)
           (java.util.concurrent.atomic AtomicLong)))

(try
  (import [org.apache.kafka.clients.consumer
           KafkaConsumer
           ConsumerRecord
           ConsumerRecords
           OffsetCommitCallback])
  (import org.apache.kafka.common.header.Header)
  (require 'asynctopia.kafka.edn)
  (catch Exception _
    (throw
      (IllegalStateException.
        "`kafka` dependency not found - aborting ..."))))

(def defaults
  {:enable.auto.commit  "false"
   :session.timeout.ms  "30000"
   :key.deserializer    "asynctopia.kafka.edn.EdnDeserializer"
   :value.deserializer  "asynctopia.kafka.edn.EdnDeserializer"
   })

(extend-protocol pclj/Datafiable
  org.apache.kafka.common.header.Header
  (datafy [this]
    [(.key this)
     (String. (.value this))])

  org.apache.kafka.clients.consumer.ConsumerRecord
  (datafy [this]
    {:topic   (.topic this)
     :key     (.key this)   ;; keyword per the (default) "key.deserializer"
     :event   (.value this) ;; EDN value per the (default) "value.deserializer"
     :headers (into {} (map df/datafy) (.headers this))
     :partition (.partition this)
     :offset  (.offset this)
     :timestamp (.timestamp this)})

  )

(defn group-by-topic
  "Higher order function that receives a set of consumer records and returns a function that expects a topic.
  The purpose of this returned function is to map each record in the set to a the topic passed as argument.
  The follwing data structure is returned:
  ```clojure
  {:topic ({:timestamp xxx :message \"some kafka message\"})}
  ```
  "
  [^org.apache.kafka.clients.consumer.ConsumerRecords records]
  (->> records
       (map df/datafy)
       (group-by :topic)))

(defn async-retry-callback
  "Per the book 'Kafka - The Definitive Guide'
   on retrying async commits."
  ^OffsetCommitCallback
  [^org.apache.kafka.clients.consumer.KafkaConsumer consumer
   ^AtomicLong id]
  (let [commit-id (.incrementAndGet id)]
    (reify org.apache.kafka.clients.consumer.OffsetCommitCallback
      (onComplete [this offsets exception]
        ;; retrying only if no other commit incremented the global counter
        (when (and (some? exception)
                   (= commit-id (.get id)))
          (.commitAsync consumer this))))))

(defn count-events
  [so-far m]
  (if (nil? m)
    so-far
    (->> (vals m)
       (map count)
       (apply + so-far))))

(defn edn-consumer
  "Creates a Kafka Consumer client with a core.async interface given the broker's list and group id.
 After the Java Kafka consumer is created it's saved in the `consumers` atom with the following format:
 ```clojure
 {:uuid {:chan core-async-input-output-channel :commit-chan core-async-commit-chnannel :consumer java-kafka-consumer}}
 ```
 A core.async process is created that polls Kafka for messages and sends them to the output channel.
 Clients must manually send a message to the commit-chan in order to continue receiving messages
 by incrementing the consumer's offset.
 This function returns the following map to the client
 ```clojure
 {:out-chan out-chan :commit-chan commit-chan :consumer-id consumer-id}
 ```
 Usage example:
 ```clojure
 (let [{out-chan :out-chan commit-chan :commit-chan} (kafka-async-consumer/create! \"localhost:9092\" \"some-group-id\" [\"topic1\"])]
    (go-loop []
       (some-processor-fn (<! out-chan))
       (>! commit-chan :kafka-commit)
       (recur)))
 ```
 "
  ([topics]
   (edn-consumer "localhost:9092" "local-consumer-group" topics))
  ([servers group-id topics]
   (edn-consumer servers group-id topics nil))
  ([servers group-id topics options]
   (edn-consumer servers group-id topics options 500))
  ([servers group-id topics options empty-interval]
   (edn-consumer servers group-id topics options empty-interval (partial println "Total:")))
  ([servers group-id topics options empty-interval polled!]
   (when-let [topics (not-empty topics)]
     (let [servers-str (if (string? servers)
                         servers
                         (str/join \, servers))
           ^Map opts (-> {:bootstrap.servers servers-str
                          :group.id          group-id
                          :client.id         "local-consumer"}
                         (merge defaults options)
                         ut/stringify-keys-1)
           retry-id    (AtomicLong. Long/MIN_VALUE)
           out-chan    (channels/chan 1)
           commit-chan (channels/chan)
           consumer    (org.apache.kafka.clients.consumer.KafkaConsumer. opts)]
       (.subscribe consumer (ArrayList. ^Collection topics))

       (ca/go-loop [polled 0]
         (let [records (try (.poll consumer (Duration/ofMillis 1))
                            (catch Exception _ :kafka/error))
               proceed? (not= :kafka/error records)
               grouped-events (when proceed? (group-by-topic records))]
           (cond
             ;; nothing to consume
             (and proceed? (empty? grouped-events))
             (do (ca/<! (ca/timeout empty-interval))
                 (recur polled))   ;; check again later
             ;; something to consume
             (and proceed?
                  (ca/>! out-chan grouped-events)  ;; put it in (always room for 1)
                  (ca/<! commit-chan))            ;; park waiting for the commit signal (any truthy value)
             (do (->> (async-retry-callback consumer retry-id)
                      (.commitAsync consumer))    ;; commit to kafka
                 (recur (count-events polled grouped-events)))

             :else
             (future ;; don't block here
               (polled! (count-events polled grouped-events))
               ;; just in case .poll() threw something other than "already closed"
               ;; might itself throw if consumer is already closed
               (.close consumer)))))

       {:out-chan    out-chan
        ;:commit-chan commit-chan
        :commit!     (partial ca/put! commit-chan :kafka/commit)
        :destroy!    (fn []
                       (ca/close! commit-chan)
                       ;; final sync commit (per the book)
                       (.commitSync consumer)
                       (.close consumer)
                       (ca/close! out-chan))}))))

(comment
  (let [{:keys [out-chan commit!]} (edn-consumer)]
    (c/consuming-with
      (fn [topic->messages]
        ;; consume (do something with) x
        (send-off topic-processor
                  (fn [state]
                    (process-topics! topic->messages)
                    (commit!) ;; commit as soon as consumed
                    (update-in state [:summary :processed] +
                               (apply + (map count (vals topic->messages)))))))
      out-chan)
    )
  )



