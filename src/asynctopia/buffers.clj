(ns asynctopia.buffers
  (:require [clojure.core.async.impl.protocols :as impl]
            [clojure.core.async.impl.dispatch :as dispatch]
            [clojure.core.async :as ca]
            [asynctopia.protocols :as proto]
            [asynctopia.util :as ut])
  (:import (java.util ArrayDeque Deque)
           (clojure.lang Counted IFn)
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

  proto/ISnapshot
  (snapshot [this] ;; NOT thread-safe!
    (ut/snapshot-buffer buf))
  )


(defn fixed-buffer
  "Drop-in (NOT thread-safe) replacement for
   `clojure.core.async.impl.buffers/fixed-buffer`
   that uses an `ArrayDeque` (rather than a `LinkedList`)
   as the underlying buffer. Do NOT pass an instance of
   `ConcurrentLinkedDeque` as the first arg (see `ts-fixed-buffer`)."
  ([n]
   (fixed-buffer nil n))
  ([^Deque dq ^long n]
   (-> (or dq (ArrayDeque. n))
       (FixedBuffer. n))))

(deftype DroppingBuffer [^Deque buf ^long n ^IFn dropped!]
  impl/UnblockingBuffer
  impl/Buffer
  (full? [this] false)
  (remove! [this]
    (.removeLast buf))
  (add!* [this itm]
    (if (>= (count this) n)
      (some-> dropped! (partial itm) dispatch/run)
      (.addFirst buf itm))
    this)
  (close-buf! [this])
  Counted
  (count [this]
    (.size buf))

  proto/IEmpty
  (clone-empty [this]
    (dropping-buffer n))

  proto/ISnapshot
  (snapshot [this] ;; NOT thread-safe!
    (ut/snapshot-buffer buf))
  )

(defn dropping-buffer
  "Drop-in (NOT thread-safe) replacement for
   `clojure.core.async.impl.buffers/dropping-buffer`
   that uses an `ArrayDeque` (rather than a `LinkedList`)
   as the underlying buffer. Do NOT pass an instance of
   `ConcurrentLinkedDeque` as the first arg (see `ts-dropping-buffer`)."
  ([n]
   (dropping-buffer n nil))
  ([n dropped!]
   (dropping-buffer nil n dropped!))
  ([^Deque dq ^long n dropped!]
   (-> (or dq (ArrayDeque. n))
       (DroppingBuffer. n dropped!))))

(deftype SlidingBuffer [^Deque buf ^long n ^IFn slided!]
  impl/UnblockingBuffer
  impl/Buffer
  (full? [this] false)
  (remove! [this]
    (.removeLast buf))
  (add!* [this itm]
    (when (= (count this) n)
      (let [slided (impl/remove! this)]
        (some-> slided! (partial slided) dispatch/run)))
    (.addFirst buf itm)
    this)
  (close-buf! [this])
  Counted
  (count [this]
    (.size buf))

  proto/IEmpty
  (clone-empty [this]
    (sliding-buffer n))

  proto/ISnapshot
  (snapshot [this] ;; NOT thread-safe!
    (ut/snapshot-buffer buf))
  )

(defn sliding-buffer
  "Drop-in (NOT thread-safe) replacement for
   `clojure.core.async.impl.buffers/sliding-buffer`
   that uses an `ArrayDeque` (rather than a `LinkedList`)
   as the underlying buffer. Do NOT pass an instance of
   `ConcurrentLinkedDeque` as the first arg (see `ts-sliding-buffer`)."
  ([n]
   (sliding-buffer n nil))
  ([n slided!]
   (sliding-buffer nil n slided!))
  ([^Deque dq ^long n slided!]
   (-> (or dq (ArrayDeque. n))
       (SlidingBuffer. n slided!))))
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
    (ut/snapshot-buffer buf))
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

(deftype ThreadSafeDroppingBuffer [^ConcurrentLinkedDeque buf cnt ^long n ^IFn dropped!]
  impl/UnblockingBuffer
  impl/Buffer
  (full? [this] false)
  (remove! [this]
    (let [x (.removeLast buf)]
      (vswap! cnt unchecked-dec)
      x))
  (add!* [this itm]
    (if (>= (count this) n)
      (some-> dropped! (partial itm) dispatch/run)
      (do (.addFirst buf itm)
          (vswap! cnt unchecked-inc)))
    this)
  (close-buf! [this])
  Counted
  (count [this] @cnt)

  proto/IEmpty
  (clone-empty [this]
    (ts-dropping-buffer n))

  proto/ISnapshot
  (snapshot [this]
    (ut/snapshot-buffer buf)))

(defn ts-dropping-buffer
  "Dropping buffer backed by a `ConcurrentLinkedDeque`."
  ([n]
   (ts-dropping-buffer n nil))
  ([n dropped!]
   (ts-dropping-buffer nil n dropped!))
  ([_ n dropped!]
   (ThreadSafeDroppingBuffer.
     (ConcurrentLinkedDeque.)
     (volatile! 0)
     n
     dropped!)))

(deftype ThreadSafeSlidingBuffer [^ConcurrentLinkedDeque buf cnt ^long n ^IFn slided!]
  impl/UnblockingBuffer
  impl/Buffer
  (full? [this] false)
  (remove! [this]
    (let [x (.removeLast buf)]
      (vswap! cnt unchecked-dec)
      x))
  (add!* [this itm]
    (when (= (count this) n)
      (let [slided (impl/remove! this)]
        (some-> slided! (partial slided) dispatch/run)))
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
    (ut/snapshot-buffer buf)))

(defn ts-sliding-buffer
  "Sliding buffer backed by a `ConcurrentLinkedDeque`."
  ([n]
   (ts-sliding-buffer n nil))
  ([n slided!]
   (ts-sliding-buffer nil n slided!))
  ([_ n slided!]
   (ThreadSafeSlidingBuffer.
     (ConcurrentLinkedDeque.)
     (volatile! 0)
     n
     slided!)))

;;===========================================================================
(defn- channel-buffer*
  [buf-or-n fixed dropping sliding]
  (cond
    (number? buf-or-n)
    (fixed buf-or-n)

    (sequential? buf-or-n)
    (let [[semantics n dq react!] buf-or-n
          n (or n 1024)]
      (case semantics
        (:fixed :buffer/fixed)       (fixed dq n)
        (:dropping :buffer/dropping) (dropping dq n react!)
        (:sliding :buffer/sliding)   (sliding dq n react!)))
    ;; assuming some instance from this namespace (or nil)
    :else buf-or-n))

(defn buf
  "Flexible/convenient ctor function for buffers (in a similar vein to `chan`)."
  ([buf-or-n]
   (buf buf-or-n false))
  ([buf-or-n thread-safe?]
   (if thread-safe?
     (channel-buffer* buf-or-n ts-fixed-buffer ts-dropping-buffer ts-sliding-buffer)
     (channel-buffer* buf-or-n fixed-buffer    dropping-buffer    sliding-buffer))))

(defn chan*
  "Drop-in replacement for `clojure.async.core/chan`, supporting
   any `Deque` buffer (not just `LinkedList`). This can be achieved
   either by pre-building the buffer via the `asynctopia.buffers` ns,
   or by providing <buf-or-n> as a vector of four elements (`[variant n dq react!]`
   where <variant> is one of :fixed/:dropping/:sliding),
   <dq> an instance of `java.util.Deque` (defaults to `ArrayDeque`),
   and <react!> a a fn of one argument (the dropped element)."
  ([]
   (chan* nil))
  ([buf-or-n]
   (chan* buf-or-n nil))
  ([buf-or-n xform]
   (chan* buf-or-n xform nil))
  ([buf-or-n xform ex-handler]
   (chan* buf-or-n xform ex-handler nil))
  ([buf-or-n xform ex-handler thread-safe-buffer?]
   (-> buf-or-n
       (buf thread-safe-buffer?)
       (ca/chan xform ex-handler))))
