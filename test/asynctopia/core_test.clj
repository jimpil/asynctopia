(ns asynctopia.core-test
  (:require [clojure.test :refer :all]
            [asynctopia.core :refer :all]
            [clojure.core.async :as ca]))

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
