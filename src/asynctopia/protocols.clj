(ns asynctopia.protocols
  (:require [clojure.core.async :as ca]
            [clojure.core.async.impl.buffers :as ca-buffers])
  (:import (clojure.core.async.impl.channels ManyToManyChannel)
           (clojure.core.async.impl.buffers FixedBuffer DroppingBuffer SlidingBuffer PromiseBuffer)))

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

;; a mere wrapper (delegates to the inner buffer)
(extend-type ManyToManyChannel
  IEmpty
  (clone-empty [this]
    ;; available to all buffers
    (clone-empty (.buf this)))
  ISnapshot
  (snapshot [this]
    ;; available to thread-safe buffers
    (snapshot (.buf this))))
