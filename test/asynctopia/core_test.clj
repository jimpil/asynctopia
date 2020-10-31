(ns asynctopia.core-test
  (:require [clojure.test :refer :all]
            [asynctopia.core :refer :all]
            [clojure.core.async :as ca]
            [asynctopia.channels :as channels])
  (:import (asynctopia.buffers FixedBuffer DroppingBuffer SlidingBuffer)))

(deftest producer-consumer
  (testing "producer-consumer"
    (let [data   (range 1000)
          errors (atom [])
          vs     (atom [])
          end    (promise)
          data-in (ca/to-chan!
                    (concat (interleave data (repeat :foo))
                            [::end]))]
      (consuming-with
        (fn [x]
          (if (= ::end x)
            (deliver end true)
            (swap! vs conj (inc x))))
        data-in
        :error! (partial swap! errors conj))
      @end
      (is (= (map inc data) @vs))
      ;; can't increment a keyword
      (is (every? (partial instance? ClassCastException) @errors)))))

(deftest thread-and-or
  (testing "thread-and"
    (is (true?  (thread-and (constantly true)
                            (constantly true))))
    (is (false? (thread-and (constantly true)
                            (constantly false))))

    ;;; prints :foo before returning false
    (let [printout (with-out-str
                     (is (false? (thread-and #(do (Thread/sleep 3000) false)
                                             #(do (Thread/sleep 1000) (println :foo))))))]
      (is (= printout (str ":foo" \newline))))
    ;;; does not print :foo
    (let [printout (with-out-str
                     (is (false? (thread-and #(do (Thread/sleep 3000) false)
                                             #(do (Thread/sleep 7000) (println :foo))))))]
      (is (empty? printout)))
    )

  (testing "thread-or"
    (is (true?  (thread-or (constantly true)
                           (constantly true))))
    (is (true?  (thread-or (constantly true)
                           (constantly false))))
    (is (false? (thread-or (constantly false)
                           (constantly false))))

    ;; prints :foo before returning true
    (let [printout (with-out-str
                     (is (true? (thread-or #(do (Thread/sleep 3000) true)
                                           #(do (Thread/sleep 1000) (println :foo))))))]
      (is (= printout (str ":foo" \newline))))

    ;; does not print :foo
    (let [printout (with-out-str
                     (is (true? (thread-or #(do (Thread/sleep 3000) true)
                                           #(do (Thread/sleep 7000) (println :foo))))))]
      (is (empty? printout)))

    )
  )

(deftest pkeep-tests
  (testing "pkeep"
    (let [data (range 10000)
          ret (ca/<!! (pkeep inc data :in-flight 4))
          expected (doall (keep inc data))]
      (is (= ret expected)))))

(deftest with-timeout-tests
  (testing "with-timeout"
    (is (= :done (ca/<!! (with-timeout 600 :timeout (Thread/sleep 500) :done))))
    (is (= :timeout (ca/<!! (with-timeout 400 :timeout (Thread/sleep 500) :done))))
    (is (nil? (ca/<!! (with-timeout 600 :timeout (Thread/sleep 500)))))
    ))

(deftest with-counting-tests
  (testing "with-counting"
    (let [data (range 1000)
          [vchan cchan] (with-counting (ca/to-chan! data))]
      (is (= data (ca/<!! (ca/into [] vchan))))
      (is (= 1000 (ca/<!! cchan))))
    )
  )

(deftest protocol-tests
  (testing "clone-buffer"
    (let [fixed-chan    (channels/chan 10)
          dropping-chan (channels/chan [:buffer/dropping 10])
          sliding-chan  (channels/chan [:buffer/sliding 10])]
      (is (instance? FixedBuffer (channel-buf (clone-channel fixed-chan))))
      (is (instance? DroppingBuffer (channel-buf (clone-channel dropping-chan))))
      (is (instance? SlidingBuffer (channel-buf (clone-channel sliding-chan))))
      ;; testing protocol extensions
      (is (instance? clojure.core.async.impl.buffers.FixedBuffer
                     (channel-buf (clone-channel (ca/chan 10)))))
      ))

  (testing "snapshot-buffer"
    (testing "sliding buffer"
      (let [slided (atom [])
            sliding-chan  (channels/chan
                            [:buffer/sliding 3 nil (partial swap! slided conj)])]
        ;; put 0,1,2
        (dotimes [i 3] (ca/>!! sliding-chan i))
        ;; put 3=>22 with interleaved assertions
        (doseq [i (range 3 23)]
          (is (= (range (- i 3) i)
                 (reverse (snapshot-channel sliding-chan))))
          (ca/>!! sliding-chan i))

        (is (= (range 20) @slided))
        (ca/close! sliding-chan)))

    (testing "dropping buffer"
      (let [dropped (atom [])
            dropping-chan  (channels/chan
                             [:buffer/dropping 3 nil (partial swap! dropped conj)])
            expected (range 3)]
        ;; put 0,1,2
        (dotimes [i 3] (ca/>!! dropping-chan i))
        ;; put 3=>23 with interleaved assertions (all dropped)
        (doseq [i (range 3 23)]
          (is (= expected (reverse (snapshot-channel dropping-chan))))
          (ca/>!! dropping-chan i))

        (is (= (range 3 23) @dropped))
        (ca/close! dropping-chan)))
    )
  )
