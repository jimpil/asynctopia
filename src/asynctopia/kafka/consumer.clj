(ns asynctopia.kafka.consumer
  (:require [clojure.core.async :as ca]
            [asynctopia.channels :as channels]
            [asynctopia.core :as c])
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
   :auto.commit.interval.ms "1000"
   :session.timeout.ms "30000"
   :key.deserializer "org.apache.kafka.common.serialization.StringDeserializer"
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
                  :message   (.value consumer-record)})
               rs)]))))

(defn async-retry-callback
  "Per the book 'Kafka - The Definitive Guide'
   on retrying async commits."
  ^OffsetCommitCallback
  [^org.apache.kafka.clients.consumer.KafkaConsumer consumer
   ^AtomicLong counter]
  (let [position (.incrementAndGet counter)]
    (proxy [OffsetCommitCallback] []
      (onComplete [offsets m exception]
        ;; retrying only if no other commit incremented the global counter
        (when (and (some? exception)
                   (= position (.get counter)))
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
   (create! servers group-id topics nil 100))
  ([servers group-id topics options poll-timeout]
   (let [^Map opts (-> {:bootstrap.servers servers
                        :group.id          group-id}
                       (merge defaults options))
         consumer    (org.apache.kafka.clients.consumer.KafkaConsumer. opts)
         retry-callback-pos (AtomicLong. Long/MIN_VALUE)
         out-chan    (channels/chan 1)
         commit-chan (channels/chan)]

     (when-let [topics (not-empty topics)]
       (.subscribe consumer (ArrayList. ^Collection topics))

       (ca/go-loop []
         (let [records (.poll consumer (Duration/ofMillis 1))
               topic->records (into {} (keep (records-by-topic records)) topics)]
           (cond
             (empty? topic->records)
             (do (ca/<! (ca/timeout poll-timeout))
                 (recur))

             (and (ca/>! out-chan topic->records)
                  (ca/<! commit-chan))
             (do (->> (async-retry-callback consumer retry-callback-pos)
                      (.commitAsync consumer))
                 (recur))
             :else
             (do (.unsubscribe consumer)
                 (.close consumer)))))

       {:out-chan    out-chan
        :commit-chan commit-chan
        :stop!       (fn []
                       (ca/close! out-chan)
                       (ca/close! commit-chan))}
       ))))

(comment
  (let [{:keys [out-chan commit-chan]} (create! )]
    (c/consuming-with
      (fn [topic->messages]
        ;; consume (do something with) x
        (send-off topic-processor
                  (fn [state]
                    (process-topics! topic->messages)
                    (ca/put! commit-chan ::commit) ;; commit as soon as consumed
                    (update-in state [:summary :processed]
                               (count (apply concat (vals topic->messages)))))))
      out-chan)
    )
  )



