(ns asynctopia.kafka-test
  (:require [clojure.test :refer :all]
            [asynctopia.kafka
             [admin :as kadmin]
             [consumer :as kconsumer]
             [producer :as kproducer]]
            [clojure.core.async :as ca]
            [clojure.java.shell :as os]
            [asynctopia.channels :as channels]
            [asynctopia.core :as c]
            [asynctopia.util :as ut]))

(defn- generate-event!
  [topics i]
  {:topic (rand-nth topics)
   :key   (str i)
   :event {:transaction ::whatever}})

(defn- process-topics!
  [topic->events]
  (pmap
    (fn [[topic events]]
      (Thread/sleep (rand-int 1000))
      (println "Processed" (count events) "events for topic" topic))
    topic->events))

(defn- init-cluster!
  []
  (if-let [kafka-home (System/getenv "KAFKA_HOME")]
    (let[{:keys [out exit err]}
         (os/sh (str kafka-home "bin/zookeeper-server-start.sh")
                (str kafka-home "config/zookeeper.properties"))
         _ (println (or err out))
         {:keys [out exit err]}
         (os/sh (str kafka-home "bin/kafka-server-start.sh")
                (str kafka-home "config/server.properties"))
         _ (println (or err out))]
      ::done)
    (throw (IllegalStateException. "$KAFKA_HOME not set!"))))

(deftest producer-consumer
  (let [;_ (init-cluster!)
        admin (kadmin/admin-client)
        topics ["fiserv" "chase" "omnipay"]
        _ (kadmin/create-topics admin topics)
        topic-processor (agent {} :error-handler ut/println-error-handler)
        stop? (atom false)
        data-in (channels/generator-chan (partial generate-event! topics)
                                         (partial rand-int 50)
                                         (count topics))
        in-chan (kproducer/edn-producer)
        {:keys [out-chan commit! destroy!]} (kconsumer/edn-consumer topics)]
    (ca/go-loop []
      (if-some [v (when-not @stop? (ca/<! data-in))]
        (when (ca/>! in-chan v)
          (recur))
        (destroy!)))

    (c/consuming-with
      (fn [topic->messages]
        ;; consume (do something with) x
        (send-off topic-processor
                  (fn [state]
                    (doall (process-topics! topic->messages))
                    (commit!) ;; commit as soon as consumed
                    (apply update-in state
                               [:summary :processed] (fnil + 0 0)
                               (map count (vals topic->messages))))))
      out-chan)

    (Thread/sleep 25000)
    (reset! stop? true)
    (Thread/sleep 500)
    (is (< 700 (get-in @topic-processor [:summary :processed])))
    (.close admin)
    )

  )