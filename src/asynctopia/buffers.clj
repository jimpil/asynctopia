(ns asynctopia.buffers
  (:require [clojure.core.async.impl.protocols :as impl]
            [asynctopia.protocols :as proto]
            [asynctopia.util :as ut])
  (:import (java.util ArrayDeque Deque)
           (clojure.lang Counted)
           (java.util.concurrent ConcurrentLinkedDeque)))

;; Drop-in buffer replacements backed by an `ArrayDeque`
;; as opposed to a `LinkedList` (still NOT thread-safe).
;; Should offer superior insertion/removal/iteration performance,
;; at the expense of memory -  the entire buffer is allocated upon creation
;; (albeit semantically more appropriate for a data-structure acting as a buffer).
(declare fixed-buffer dropping-buffer sliding-buffer)

(deftype FixedBuffer [^Deque buf ^long n]
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

  proto/IEmpty
  (clone-empty [this]
    (fixed-buffer n))
  )


(defn fixed-buffer
  "Drop-in (NOT thread-safe) replacement for
   `clojure.core.async.impl.buffers/fixed-buffer`
   that uses an `ArrayDeque` (rather than a `LinkedList`)
   as the underlying buffer."
  ([n]
   (fixed-buffer nil n))
  ([^Deque dq ^long n]
   (-> (or dq (ArrayDeque. n))
       (FixedBuffer. n))))

(deftype DroppingBuffer [^Deque buf ^long n]
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

  proto/IEmpty
  (clone-empty [this]
    (dropping-buffer n))
  )

(defn dropping-buffer
  "Drop-in (NOT thread-safe) replacement for
   `clojure.core.async.impl.buffers/dropping-buffer`
   that uses an `ArrayDeque` (rather than a `LinkedList`)
   as the underlying buffer."
  ([n]
   (dropping-buffer nil n))
  ([^Deque dq ^long n]
   (-> (or dq (ArrayDeque. n))
       (DroppingBuffer. n))))

(deftype SlidingBuffer [^Deque buf ^long n]
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

  proto/IEmpty
  (clone-empty [this]
    (sliding-buffer n))
  )

(defn sliding-buffer
  "Drop-in (NOT thread-safe) replacement for
   `clojure.core.async.impl.buffers/sliding-buffer`
   that uses an `ArrayDeque` (rather than a `LinkedList`)
   as the underlying buffer."
  ([^long n]
   (sliding-buffer n))
  ([^Deque dq ^long n]
   (-> (or dq (ArrayDeque. n))
       (SlidingBuffer. n))))
;;==============================================================================
;;------------------------------------------------------------------------------
;;==============================================================================
;; Drop-in buffer replacements backed by an `ConcurrentLinkedDeque` (thread-safe)
(declare ts-fixed-buffer ts-dropping-buffer ts-sliding-buffer)

(deftype ThreadSafeFixedBuffer [^ConcurrentLinkedDeque buf cnt ^long n]
  impl/Buffer
  (full? [this]
    (>= (count this) n))
  (remove! [this]
    (let [x (.removeLast buf)]
      (vswap! cnt unchecked-dec)
      x))
  (add!* [this itm]
    (.addFirst buf itm)
    (vswap! cnt unchecked-inc)
    this)
  (close-buf! [this])
  Counted
  (count [this] @cnt)

  proto/IEmpty
  (clone-empty [this]
    (ts-fixed-buffer n))

  proto/ISnapshot
  (snapshot [this]
    (ut/snapshot-java-collection buf))
  )

(defn ts-fixed-buffer
  "Fixed buffer backed by a `ConcurrentLinkedDeque`."
  ([n]
   (ts-fixed-buffer nil n))
  ([_ n]
   (ThreadSafeFixedBuffer.
     (ConcurrentLinkedDeque.)
     (volatile! 0)
     n)))

(deftype ThreadSafeDroppingBuffer [^ConcurrentLinkedDeque buf cnt ^long n]
  impl/UnblockingBuffer
  impl/Buffer
  (full? [this] false)
  (remove! [this]
    (let [x (.removeLast buf)]
      (vswap! cnt unchecked-dec)
      x))
  (add!* [this itm]
    (when-not (>= (count this) n)
      (.addFirst buf itm)
      (vswap! cnt unchecked-inc))
    this)
  (close-buf! [this])
  Counted
  (count [this] @cnt)

  proto/IEmpty
  (clone-empty [this]
    (ts-dropping-buffer n))

  proto/ISnapshot
  (snapshot [this]
    (ut/snapshot-java-collection buf)))

(defn ts-dropping-buffer
  "Dropping buffer backed by a `ConcurrentLinkedDeque`."
  ([n]
   (ts-dropping-buffer nil n))
  ([_ n]
   (ThreadSafeDroppingBuffer.
     (ConcurrentLinkedDeque.)
     (volatile! 0)
     n)))

(deftype ThreadSafeSlidingBuffer [^ConcurrentLinkedDeque buf cnt ^long n]
  impl/UnblockingBuffer
  impl/Buffer
  (full? [this] false)
  (remove! [this]
    (let [x (.removeLast buf)]
      (vswap! cnt unchecked-dec)
      x))
  (add!* [this itm]
    (when (= (count this) n)
      (impl/remove! this))
    (.addFirst buf itm)
    (vswap! cnt unchecked-inc)
    this)
  (close-buf! [this])
  Counted
  (count [this] @cnt)

  proto/IEmpty
  (clone-empty [this]
    (ts-sliding-buffer n))

  proto/ISnapshot
  (snapshot [this]
    (ut/snapshot-java-collection buf)))

(defn ts-sliding-buffer
  "Sliding buffer backed by a `ConcurrentLinkedDeque`."
  ([n]
   (ts-sliding-buffer nil n))
  ([_ n]
   (ThreadSafeSlidingBuffer.
     (ConcurrentLinkedDeque.)
     (volatile! 0)
     n)))

(defn snapshot-buffer
  "Returns the (current) contents of this channel's (thread-safe) buffer."
  [ch]
  (proto/snapshot (ut/channel-buffer ch)))
;;===========================================================================
(defn- buffer*
  [buf-or-n fixed dropping sliding]
  (cond
    (nil? buf-or-n)    (fixed 1024)
    (number? buf-or-n) (fixed buf-or-n)
    (sequential? buf-or-n)
    (let [[semantics n dq] buf-or-n]
      (case semantics
        :fixed    (fixed dq n)
        :dropping (dropping dq n)
        :sliding  (sliding dq n)))
    ;; assuming some instance from this namespace
    :else buf-or-n))

(defn buffer
  "Flexible/convenient ctor function for buffers."
  ([buf-or-n]
   (buffer buf-or-n false))
  ([buf-or-n thread-safe?]
   (if thread-safe?
     (buffer* buf-or-n ts-fixed-buffer ts-dropping-buffer ts-sliding-buffer)
     (buffer* buf-or-n fixed-buffer    dropping-buffer    sliding-buffer))))
