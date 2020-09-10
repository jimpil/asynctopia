(ns asynctopia.buffers.array
  (:require [clojure.core.async.impl.protocols :as impl]
            [clojure.core.async :as ca])
  (:import (java.util ArrayDeque)
           (clojure.lang Counted)))

;; Drop-in buffer replacements backed by an `ArrayDeque`
;; as opposed to a `LinkedList` (still NOT thread-safe).
;; Should offer superior insertion/removal/iteration performance,
;; at the expense of memory -  the entire buffer is allocated upon creation
;; (albeit semantically more appropriate for a data-structure acting as a buffer).

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


(defn fixed-buffer
  "Drop-in (NOT thread-safe) replacement for
   `clojure.core.async.impl.buffers/fixed-buffer`
   that uses an `ArrayDeque` (rather than a `LinkedList`)
   as the underlying buffer."
  [n]
  ;; pre-allocate the whole array needed
  ;; in order to avoid resizing in the future
  (-> (ArrayDeque. (int n))
      (FixedBuffer. n)))

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

(defn dropping-buffer
  "Drop-in (NOT thread-safe) replacement for
   `clojure.core.async.impl.buffers/dropping-buffer`
   that uses an `ArrayDeque` (rather than a `LinkedList`)
   as the underlying buffer."
  [n]
  ;; pre-allocate the whole array needed
  ;; in order to avoid resizing in the future
  (-> (ArrayDeque. (int n))
      (DroppingBuffer. n)))

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

(defn sliding-buffer
  "Drop-in (NOT thread-safe) replacement for
   `clojure.core.async.impl.buffers/sliding-buffer`
   that uses an `ArrayDeque` (rather than a `LinkedList`)
   as the underlying buffer."
  [n]
  ;; pre-allocate the whole array needed
  ;; in order to avoid resizing in the future
  (-> (ArrayDeque. (int n))
      (SlidingBuffer. n)))

(defn- array-backed-buffer
  [buf-or-n]
  (cond
    (number? buf-or-n)
    (fixed-buffer buf-or-n)

    (sequential? buf-or-n)
    (let [[buf n] buf-or-n]
      (case buf
        :fixed    (fixed-buffer n)
        :dropping (dropping-buffer n)
        :sliding  (sliding-buffer n)))

    ;; assuming some instance from this namespace
    :else buf-or-n)
  )

(defn array-buffer-chan
  "Drop-in replacement for `clojure.async.core/chan`,
   but with array-backed (instead of linked-list) buffer.
   <buf-or-n> can also be provided as a vector of two elements
   (`[variant n]` where variant is one of :fixed/:dropping/:sliding)."
  ([]
   (array-buffer-chan nil))
  ([buf-or-n]
   (array-buffer-chan buf-or-n nil))
  ([buf-or-n xform]
   (array-buffer-chan buf-or-n xform nil))
  ([buf-or-n xform ex-handler]
   (-> (array-backed-buffer buf-or-n)
       (ca/chan xform ex-handler))))
