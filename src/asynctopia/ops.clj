(ns asynctopia.ops
  (:require [clojure.core.async :as ca])
  (:import (clojure.core.async.impl.buffers FixedBuffer DroppingBuffer SlidingBuffer)))

(defn pipe-with
  "Pipes the <from> channel into a newly created output-channel
   (created with `(chan buffer (keep f) to-error)`), returning the latter."
  [f from & {:keys [to-error buffer]
             :or {to-error identity
                  buffer 1024}}]
  ;; returns the `to` channel (2nd arg)
  (ca/pipe from (ca/chan buffer (keep f) to-error)))

(defmacro pipe1
  "Pipes an element from the <from> channel and supplies it to the <to>
   channel. The to channel will be closed when the from channel closes.
   Must be called within a go block."
  [from to]
  `(if-some [v# (ca/<! ~from)]
     (ca/>! ~to v#)
     (ca/close! ~to)))

(defmacro go-consume!
  ""
  [f c]
  `(ca/go-loop []
     (when-some [x# (ca/<! ~c)]
       (~f x#)
       (recur))))

(defn merge-reduce
  "If no <chans> are provided, essentially a wrapper to `ca/reduce`,
   otherwise merges all <chans> and reduces them with <f>, <init>."
  [f init chan & chans]
  (->> (if (seq chans)
         (ca/merge (cons chan chans))
         chan)
       (ca/reduce f init)))

(defn clone-buffer
  "If <buffer> is a integer, simply returns it.
   Otherwise, if it's an instance of `FixedBuffer`,
   `DroppingBuffer`, or `SlidingBuffer`, returns a new
   instance of the same type and the same  `n`."
  [buffer]
  (let [bc (class buffer)]
    (case bc
      (Integer, Long) (if (pos? buffer)
                        buffer
                        (throw
                          (IllegalArgumentException.
                            "A buffer must have capacity!")))
      FixedBuffer    (ca/buffer          (:n buffer))
      DroppingBuffer (ca/dropping-buffer (:n buffer))
      SlidingBuffer  (ca/sliding-buffer  (:n buffer))
      (throw
        (IllegalArgumentException.
          (str "Unsupported buffer class: " bc))))))
