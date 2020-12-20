(ns asynctopia.protocols
  (:require [clojure.core.async :as ca]
            [asynctopia.util :as ut]
            [clojure.core.async.impl.buffers :as ca-buffers])
  (:import (clojure.core.async.impl.buffers FixedBuffer
                                            DroppingBuffer
                                            SlidingBuffer
                                            PromiseBuffer)))

(defprotocol IEmpty
  (clone-empty [this]))

(defprotocol ISnapshot
  (snapshot [this]))

;; extend to the core.async buffers
(extend-protocol IEmpty
  FixedBuffer
  (clone-empty [b]
    (ca/buffer (.n b)))
  DroppingBuffer
  (clone-empty [b]
    (ca/dropping-buffer (.n b)))
  SlidingBuffer
  (clone-empty [b]
    (ca/sliding-buffer (.n b)))
  PromiseBuffer
  (clone-empty [_]
    (ca-buffers/promise-buffer))
  )

(extend-protocol ISnapshot
  ;; NOT thread-safe buffers - be extra careful!
  FixedBuffer
  (snapshot [b]
    (ut/snapshot-buffer (.buf b)))
  DroppingBuffer
  (snapshot [b]
    (ut/snapshot-buffer (.buf b)))
  SlidingBuffer
  (snapshot [b]
    (ut/snapshot-buffer (.buf b)))
  PromiseBuffer
  (snapshot [b]
    [(.val b)])
  )
