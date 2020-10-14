(ns asynctopia.kafka-test
  (:require [clojure.test :refer :all]
            [asynctopia.kafka
             [consumer :as kconsumer]
             [producer :as kproducer]]
            [clojure.core.async :as ca]
            [asynctopia.channels :as channels]
            [asynctopia.core :as c]))

(defn- generate-event!
  [topics i]
  {:topic (rand-nth topics)
   :key  i
   :event {:transaction ::whatever}})

(defn- process-topics!
  [topic->events]
  (pmap
    (fn [[topic events]]
      (Thread/sleep (rand-int 1000))
      (println "Processed" (count events) "events for topic" topic))
    topic->events))

(deftest producer-consumer
  (let [topics ["fiserv" "chase" "omnipay"]
        topic-processor (agent {})
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
                    (update-in state
                               [:summary :processed] (fnil + 0)
                               (map count (vals topic->messages))))))
      out-chan)

    (Thread/sleep 10000)
    (reset! stop? true)
    (Thread/sleep 100)
    ;(is )

    )

  )