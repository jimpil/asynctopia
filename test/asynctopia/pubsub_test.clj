(ns asynctopia.pubsub-test
  (:require [clojure.test :refer :all]
            [asynctopia.pubsub :refer :all]
            [clojure.core.async :as ca]
            [asynctopia.channels :as channels]))

(deftest pub-sub-tests
  (testing "pub-sub"
    (let [topics [:t1 :t2 :t3 :t4 :t5]
          messages (vec (range 1000))
          results (atom [])
          end (promise)
          topic-processor (fn dummy [topic payload]
                            (if (= :end payload)
                              (deliver end true)
                              (case topic
                                (:t1 :t2 :t3 :t4 :t5)
                                (swap! results conj payload)))
                            (println "Processed Message:" topic payload))
          config {:topics topics
                  :topic-fn :topic
                  :payload-fn :message
                  :multi-process! topic-processor}
          N 50
          [pb in-chan sub-chans] (-> (fn [i]
                                       {:topic (rand-nth topics)
                                        :message (if (= N i) :end (rand-nth messages))})
                                     (channels/gen-chan (constantly 30))
                                     (pub-sub! config))]
      @end
      (ca/close! in-chan)
      (is (= N (count @results)))
      )
    )
  )
