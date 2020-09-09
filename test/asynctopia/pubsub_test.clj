(ns asynctopia.pubsub-test
  (:require [clojure.test :refer :all]
            [asynctopia.pubsub :refer :all]
            [clojure.core.async :as ca]))

(deftest pub-sub-tests
  (testing "pub-sub"
    (let [topics [:t1 :t2 :t3 :t4 :t5]
          messages (range 10000)
          topic-fn :topic
          results (atom #{})
          topic-processor (fn [topic message]
                            (println "Processing Message:" message)
                            (case topic
                              (:t1 :t2 :t3 :t4 :t5)
                              (swap! results conj message)))
          input (ca/to-chan!
                  (repeatedly 2000 (fn []
                                     {:topic (rand-nth topics)
                                      :message (rand-nth messages)})))

          [pb sub-chans] (pub-sub! input
                                   topic-fn
                                   [topics topic-processor])]
      (Thread/sleep 2000)
      (is (>= 2000 (count @results)))
      )
    )
  )
