(ns asynctopia.array-buffers
  (:require [clojure.core.async.impl.protocols :as impl])
  (:import (java.util ArrayDeque)
           (clojure.lang Counted)))

;; Drop-in buffer replacements, backed by an `ArrayDeque`
;; as opposed to a `LinkedList` (still NOT thread-safe).
;; Should offer superior insertion/removal/iteration performance,
;; at the expense of memory (the entire buffer is allocated upon creation,
;; which is actually more appropriate for a buffer).

(deftype FixedBuffer [^ArrayDeque buf ^long n]
  impl/Buffer
  (full? [this]
    (>= (count this) n))
  (remove! [this]
    (.removeLast buf))
  (add!* [this itm]
    (.addFirst buf itm)
    this)
  (close-buf! [this])
  Counted
  (count [this]
    (.size buf))
  )

(defn fixed-buffer [n]
  (FixedBuffer. (ArrayDeque. (int n)) n))

(deftype DroppingBuffer [^ArrayDeque buf ^long n]
  impl/UnblockingBuffer
  impl/Buffer
  (full? [this] false)
  (remove! [this]
    (.removeLast buf))
  (add!* [this itm]
    (when-not (>= (count this) n)
      (.addFirst buf itm))
    this)
  (close-buf! [this])
  Counted
  (count [this]
    (.size buf))
  )

(defn dropping-buffer [n]
  (DroppingBuffer. (ArrayDeque. (int n)) n))

(deftype SlidingBuffer [^ArrayDeque buf ^long n]
  impl/UnblockingBuffer
  impl/Buffer
  (full? [this] false)
  (remove! [this]
    (.removeLast buf))
  (add!* [this itm]
    (when (= (count this) n)
      (impl/remove! this))
    (.addFirst buf itm)
    this)
  (close-buf! [this])
  Counted
  (count [this]
    (.size buf))
  )

(defn sliding-buffer [n]
  (SlidingBuffer. (ArrayDeque. (int n)) n))
