(ns asynctopia.pubsub-test
  (:require [clojure.test :refer :all]
            [asynctopia.pubsub :refer :all]
            [clojure.core.async :as ca]
            [asynctopia.channels :as channels]
            [asynctopia.null :as null]))

(deftest pub-sub-tests
  (testing "pub-sub!"
    (let [topics [:t1 :t2 :t3 :t4 :t5]
          messages (vec (range 1000))
          N 50
          ;end (promise)
          results (ca/chan N)
          topic-processor (fn dummy [topic payload]
                            (if-some [pl (null/restoring payload)]
                              (do
                                (case topic
                                  (:t1 :t2 :t3 :t4 :t5)
                                  (ca/put! results pl))
                                (println "Processed Message:" topic pl))
                              (ca/close! results)))
          config {:topics topics
                  :topic-fn :topic
                  :payload-fn :message
                  :multi-buffer (constantly 1)
                  :multi-process! topic-processor}
          [pb in-chan sub-chans] (-> (fn [i]
                                       {:topic (rand-nth topics)
                                        :message (when (< i N) (rand-nth messages))})
                                     (channels/gen-chan nil N)
                                     (pub-sub! config))]

      (is (= N (count (ca/<!! (ca/into [] results)))))
      (ca/close! in-chan)
      )
    )
  )
