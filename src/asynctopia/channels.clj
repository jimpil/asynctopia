(ns asynctopia.channels
  (:require [clojure.core.async :as ca]
            [clojure.java.io :as io]
            [asynctopia
             [ops :as ops]
             [util :as ut]]
            [asynctopia.protocols :as proto]
            [clojure.core.async.impl.buffers :as buffers])
  (:import (clojure.core.async.impl.buffers FixedBuffer DroppingBuffer SlidingBuffer PromiseBuffer)))

(defn line-chan
  "Returns a channel that will receive all the lines
   in <src> (via `line-seq`) transformed per <xform>."
  ([src]
   (line-chan src 1024))
  ([src buf-or-n]
   (line-chan src buf-or-n (map identity)))
  ([src buf-or-n xform]
   (line-chan src buf-or-n xform ut/println-error-handler))
  ([src buf-or-n xform ex-handler]
   (let [out-chan (ca/chan buf-or-n xform ex-handler)]
     (ca/go
       (with-open [rdr (io/reader src)]
         (doseq [line (line-seq rdr)]
           (ca/>! out-chan line)))
       (ca/close! out-chan))
     out-chan)))

(defn counting-chan
  "Returns a channel that will receive the
   total number of elements taken from <ch>."
  [ch]
  (ca/go-loop [n 0]
    (if-some [_ (ca/<! ch)]
      (recur (inc n))
      n)))

(comment
  ;; process the non-empty lines from <src> with <f> (one-by-one)
  ;; and report number of errors VS successes
  (->> (line-chan src 1024 (comp (remove empty?) (keep f)) identity)
       (ca/split ut/throwable?)
       (map counting-chan)
       (map ca/<!!) ;; => (0 120000)
       )

  )

(defn- round [n]
  (Math/round (double n)))

(defn- chan-throttler* [rate-ms bucket-size]
  (let [sleep-time (round (max (/ rate-ms) 10))
        token-value (long (round (* sleep-time rate-ms)))   ; how many messages to pipe per token
        bucket (ca/chan (ca/dropping-buffer bucket-size))] ; we model the bucket with a buffered channel

    ;; The bucket filler loop. Puts a token in the bucket every
    ;; sleep-time seconds. If the bucket is full the token is dropped.
    (ca/go
      (while (ca/>! bucket ::token)
        (ca/<! (ca/timeout (long sleep-time)))))

    ;; The piping loop. Takes a token from the bucket (parking until
    ;; one is ready if the bucket is empty), and forwards token-value
    ;; messages from the source channel to the output channel.

    ;; For high frequencies, we leave sleep-time fixed to
    ;; min-sleep-time, and we increase token-value, the number of
    ;; messages to pipe per token. For low frequencies, the token-value
    ;; is 1 and we adjust sleep-time to obtain the desired rate.

    (fn [c]
      (let [tc (ca/chan)] ; the throttled chan
        (ca/go
          (while (ca/<! bucket) ; park for a token
            (dotimes [_ (long token-value)]
              (when-not (ops/pipe1 c tc)
                (ca/close! bucket)))))
        tc))))

(defn- chan-throttler
  "Returns a function that will take an input channel and return an
   output channel with the desired rate. Optionally accepts a bucket size
   for bursty channels.
   If the throttling function returned here is used on more than one
   channel, they will all share the same token-bucket. This means their
   overall output rate combined will be equal to the provided rate. In
   other words, they will all share the allocated bandwidth using
   statistical multiplexing."

  ([rate unit]
   (chan-throttler rate unit 1))
  ([rate unit bucket-size]
   (when-not (contains? ut/unit->ms unit)
     (throw (IllegalArgumentException.
              (str "Invalid unit! Available units are: " (keys ut/unit->ms)))))

   (when-not (and (number? rate) (pos? rate))
     (throw (IllegalArgumentException. "<rate> should be a positive number!")))

   (when-not (pos-int? bucket-size)
     (throw (IllegalArgumentException. "<bucket-size> MUST be a positive integer!")))

   (let [rate-ms (/ rate (ut/unit->ms unit))]
     (chan-throttler* rate-ms bucket-size))))


(defn throttled-chan
  "Takes a write channel, a goal rate and a unit and returns a read
   channel. Messages written to the input channel can be read from
   the throttled output channel at a rate that will be at most the
   provided goal rate.
   Optionally takes a bucket size, which will correspond to the
   maximum number of burst messages.
   As an example, the channel produced by calling:
   (throttle-chan (chan) 1 :second 9)
   Will transmit 1 message/second on average but can transmit up to
   10 messages on a single second (9 burst messages + 1
   message/second).
   Note that after the burst messages have been consumed they have to
   be refilled in a quiescent period at the provided rate, so the
   overall goal rate is not affected in the long term.
   The throttled channel will be closed when the input channel closes."
  ([c rate unit]
   (throttled-chan c rate unit 1))
  ([c rate unit bucket-size]
   ((chan-throttler rate unit bucket-size) c)))


(extend-protocol proto/IEmptyBuffer
  FixedBuffer
  (clone-empty [b]  (ca/buffer (.n b)))
  DroppingBuffer
  (clone-empty [b] (ca/dropping-buffer (.n b)))
  SlidingBuffer
  (clone-empty [b] (ca/sliding-buffer (.n b)))
  PromiseBuffer
  (clone-empty [_] (buffers/promise-buffer))
  )

(defn empty-buffer
  "Returns a new/empty buffer of the same type and (buffering) capacity
   as the provided channel's buffer."
  [ch]
  (proto/clone-empty (ut/channel-buffer ch)))


