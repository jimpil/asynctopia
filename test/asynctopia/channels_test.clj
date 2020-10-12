(ns asynctopia.channels-test
  (:require [clojure.test :refer :all]
            [asynctopia.channels :refer :all]
            [clojure.core.async :as ca]
            [asynctopia.util :as ut]
            [asynctopia.core :as core])
  (:import (java.util.stream LongStream)))

(deftest onto-chan-tests
  (testing "onto-chan!!"
    (testing "against reducible input"
      (let [data (range 1000)
            out-chan (chan 1000)]
        (onto-chan!! out-chan data)
        (is (ut/reducible? data))
        (is (= data (ca/<!! (ca/into [] out-chan)))))
      )
    )
  )

(deftest stream-chan-tests
  (testing "stream-chan"
    (let [data (range 1000)
          stream (LongStream/range 0 1000)
          out-chan (stream-chan stream)]
      (is (= data (ca/<!! (ca/into [] out-chan))))))
  )

(deftest gen-chan-tests
  (testing "gen-chan"
    (let [xs [:foo :bar :baz]
          gen! (fn [_] (rand-nth xs))
          interval! (partial rand-int 100)
          gchan (generator-chan gen! interval!)
          [mg cnt-chan] (core/with-counting gchan)]
      (future
        (Thread/sleep 3000)
        (ca/close! gchan))

      (let [ret (ca/<!! (ca/into [] mg))]
        (is (not-empty ret))
        (is (every? (set xs) ret)))

      (is (pos? (ca/<!! cnt-chan)))))
  )