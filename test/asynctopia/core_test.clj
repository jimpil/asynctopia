(ns asynctopia.core-test
  (:require [clojure.test :refer :all]
            [asynctopia.core :refer :all]
            [clojure.core.async :as ca]))

(deftest producer-consumer
  (testing "producer-consumer"
    (let [data   (range 1000)
          errors (atom [])
          vs     (atom [])
          data-in (ca/to-chan! (interleave data (repeat :foo)))]
      (consuming-with
        (fn [x]
          (swap! vs conj (inc x)))
        data-in
        :error! (partial swap! errors conj))
      (Thread/sleep 2000)
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
          ret (pkeep inc data :in-flight 4)
          expected (doall (keep inc data))]
      (is (= @ret expected)))))

(deftest with-timeout-tests
  (testing "with-timeout"
    (is (= :done @(with-timeout 600 :timeout (Thread/sleep 500) :done)))
    (is (= :timeout @(with-timeout 400 :timeout (Thread/sleep 500) :done)))
    (is (nil? @(with-timeout 600 :timeout (Thread/sleep 500))))
    )

  )
