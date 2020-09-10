(ns asynctopia.buffers.thread-safe
  (:require [clojure.core.async.impl.protocols :as impl]
            [asynctopia.protocols :as proto]
            [asynctopia.util :as ut])
  (:import (java.util.concurrent ConcurrentLinkedDeque)
           (clojure.lang Counted)))

;; Drop-in buffer replacements backed by a ConcurrentLinkedDeque
;; (as opposed to a LinkedList). This enabled the buffer to be
;; traversed (while live) w/o throwing a ConcurrentModificationException,
;; thus making it snapshot-able (see `snapshot-buffer`).

(deftype FixedBuffer [^ConcurrentLinkedDeque buf cnt ^long n]
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

  proto/ISnapshotBuffer
  (snapshot [this]
    (seq (.toArray buf)))
  )

(defn fixed-buffer
  [^long n]
  (FixedBuffer.
    (ConcurrentLinkedDeque.)
    (volatile! 0)
    n))

(deftype DroppingBuffer [^ConcurrentLinkedDeque buf cnt ^long n]
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

  proto/ISnapshotBuffer
  (snapshot [this]
    (seq (.toArray buf))))

(defn dropping-buffer
  [n]
  (DroppingBuffer.
    (ConcurrentLinkedDeque.)
    (volatile! 0)
    n))

(deftype SlidingBuffer [^ConcurrentLinkedDeque buf cnt ^long n]
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

  proto/ISnapshotBuffer
  (snapshot [this]
    (seq (.toArray buf))))

(defn sliding-buffer
  [n]
  (SlidingBuffer.
    (ConcurrentLinkedDeque.)
    (volatile! 0)
    n))

(defn snapshot-buffer
  "Returns the (current) contents of this channel's (thread-safe) buffer."
  [concurrent-buffer-ch]
  (proto/snapshot (ut/channel-buffer concurrent-buffer-ch)))
