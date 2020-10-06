(ns asynctopia.channels-test
  (:require [clojure.test :refer :all]
            [asynctopia.channels :refer :all]
            [clojure.core.async :as ca]
            [asynctopia.util :as ut])
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