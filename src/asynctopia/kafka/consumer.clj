(ns asynctopia.kafka.consumer
  (:require [clojure.core.async :as ca]
            [asynctopia.channels :as channels]
            [asynctopia.core :as c]
            [clojure.walk :as walk]
            [clojure.edn :as edn])
  (:import (java.util Map ArrayList Collection)
           (java.time Duration)
           (org.apache.kafka.clients.consumer OffsetCommitCallback)
           (java.util.concurrent.atomic AtomicLong)))

(try
  (import org.apache.kafka.clients.consumer.KafkaConsumer)
  (import org.apache.kafka.clients.consumer.ConsumerRecord)
  (import org.apache.kafka.clients.consumer.ConsumerRecords)
  (catch Exception _
    (throw
      (IllegalStateException.
        "`kafka` dependency not found - aborting ..."))))

(def defaults
  {:enable.auto.commit  "false"
   :session.timeout.ms "30000"
   :key.deserializer   "org.apache.kafka.common.serialization.StringDeserializer"
   :value.deserializer "org.apache.kafka.common.serialization.StringDeserializer"
   })

(defn records-by-topic
  "Higher order function that receives a set of consumer records and returns a function that expects a topic.
  The purpose of this returned function is to map each record in the set to a the topic passed as argument.
  The follwing data structure is returned:
  ```clojure
  {:topic ({:timestamp xxx :message \"some kafka message\"})}
  ```
  "
  [^org.apache.kafka.clients.consumer.ConsumerRecords records]
  (fn [^String topic]
    (let [consumer-records (.records records topic)]
      (when-let [rs (seq consumer-records)]
        [(keyword topic)
         (mapv (fn [^org.apache.kafka.clients.consumer.ConsumerRecord consumer-record]
                 {:timestamp (.timestamp consumer-record)
                  :message   (edn/read-string (.value consumer-record)})
               rs)]))))

(defn async-retry-callback
  "Per the book 'Kafka - The Definitive Guide'
   on retrying async commits."
  ^OffsetCommitCallback
  [^org.apache.kafka.clients.consumer.KafkaConsumer consumer
   ^AtomicLong id]
  (let [commit-id (.incrementAndGet id)]
    (proxy [OffsetCommitCallback] []
      (onComplete [offsets m exception]
        ;; retrying only if no other commit incremented the global counter
        (when (and (some? exception)
                   (= commit-id (.get id)))
          (.commitAsync consumer this))))))

(defn create!
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
  ([servers group-id topics]
   (create! servers group-id topics nil))
  ([servers group-id topics options]
   (create! servers group-id topics options 500))
  ([servers group-id topics options empty-interval]
   (create! servers group-id topics options empty-interval
            (partial println "Total consumed:")))
  ([servers group-id topics options empty-interval consumed!]
   (create! servers group-id topics options empty-interval consumed!
            (partial println "Dropping kafka consumer-records prior to final commit:")))
  ([servers group-id topics options empty-interval consumed! dropping!]
   (let [^Map opts (-> {:bootstrap.servers servers
                        :group.id          group-id}
                       (merge defaults options)
                       walk/stringify-keys)
         consumer    (org.apache.kafka.clients.consumer.KafkaConsumer. opts)
         retry-id    (AtomicLong. Long/MIN_VALUE)
         out-chan    (channels/chan 1)
         commit-chan (channels/chan)]

     (when-let [topics (not-empty topics)]
       (.subscribe consumer (ArrayList. ^Collection topics))

       (ca/go-loop [consumed 0]
         (let [records (try (.poll consumer (Duration/ofMillis 1))
                            (catch Exception _ ::abort))
               proceed? (not= ::abort records)
               topic->records (when proceed?
                                (into {} (keep (records-by-topic records)) topics))]
           (cond
             ;; nothing to consume
             (and proceed? (empty? topic->records))
             (do (ca/<! (ca/timeout empty-interval))
                 (recur consumed))   ;; check again later
             ;; something to consume
             (and proceed?
                  (ca/>! out-chan topic->records) ;; put it in (first put is guaranteed to succeed)
                  (ca/<! commit-chan))            ;; park waiting for the commit signal (any truthy value)
             (do (->> (async-retry-callback consumer retry-id)
                      (.commitAsync consumer))    ;; commit to kafka
                 (recur (->> (vals topic->records)
                             (map count)
                             (apply + consumed))))
             :else ;; about to exit the loop - close everything
             (do (ca/close! out-chan)       ;; noop when already closed
                 (ca/close! commit-chan)    ;; noop when already closed
                 (consumed! consumed)       ;; report on totals?
                 (dropping! topic->records) ;; report on potentially dropped records?
                 (.commitSync consumer)     ;; final commit
                 (.close consumer)))))      ;; close kafka consumer

       {:out-chan    out-chan
        ;:commit-chan commit-chan
        :commit!     (partial ca/put! commit-chan :kafka/commit)
        :destroy!    (fn []
                       (ca/close! out-chan)
                       (ca/close! commit-chan))}
       ))))

(comment
  (let [{:keys [out-chan commit!]} (create! )]
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



