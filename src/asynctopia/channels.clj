(ns asynctopia.channels
  (:require [clojure.core.async :as ca]
            [clojure.core.async.impl.buffers :as ca-buffers]
            [clojure.java.io :as io]
            [clambda.core :as clambda]
            [asynctopia
             [protocols :as proto]
             [ops :as ops]
             [buffers :as buffers]
             [util :as ut]])
  (:import (clojure.core.async.impl.buffers FixedBuffer
                                            DroppingBuffer
                                            SlidingBuffer
                                            PromiseBuffer)
           (java.io BufferedReader)
           (java.util.stream Stream)
           (java.nio.file Files Path)))

(def chan
  "See `buffers/chan*`"
  buffers/chan*)

(defn- done-with*
  [done! ch]
  (cond
    ;; booleans work (backwards compatible)
    (true? done!)
    (ca/close! ch)
    ;; functions work
    (fn? done!)
    (done! ch)))

(defn onto-chan!!
  "Drop-in replacement for `ca/onto-chan!!`, with a more flexible 3rd
   argument - allowing for a function which will be called with <ch>
   when <coll> is exhausted (defaults to `ca/close` which is effectively
   the same as `true`).It is also nil-safe (see the `asynctopia.null` convention),
   and reducible-friendly (i.e. <coll> can be something implementing `IReduceInit`)."
  ([ch coll]
   (onto-chan!! ch coll true))
  ([ch coll done!]
   (if (ut/reducible? coll)
     ;; something reducible
     (ca/thread
       (reduce
         (fn [more? x]
           (if more?
             (ops/>!!? ch x)
             (reduced false)))
         true
         coll)
       (done-with* done! ch))
     ;; regular Seq
     (ca/thread
       (loop [vs (seq coll)]
         (if (and vs (ops/>!!? ch (first vs)))
           (recur (next vs))
           (done-with* done! ch)))))))

(defn onto-chan!
  "Drop-in replacement for `ca/onto-chan!`, with a more flexible 3rd
   argument - allowing for a function which will be called with <ch>
   when <coll> is exhausted (defaults to `ca/close` which is effectively
   the same as `true`). It is also nil-safe (see the `asynctopia.null` convention),
   and reducible-friendly (by falling back to `onto-chan!!`)."
  ([ch coll]
   (onto-chan! ch coll true))
  ([ch coll done!]
   (if (ut/reducible? coll)
     ;; something reducible - cannot use `go` - degrade to `onto-chan!!`
     (onto-chan!! ch coll done!)
     ;; regular Seq
     (ca/go-loop [vs (seq coll)]
       (if (and vs (ops/>!? ch (first vs)))
         (recur (next vs))
         (done-with* done! ch))))))

(defn stream-chan!!
  "Returns a channel that will receive all the
   elements in Stream <src> (via `onto-chan!!`)
   transformed per <xform>, and then close."
  ([src]
   (stream-chan!! src nil))
  ([src buf-or-n]
   (stream-chan!! src buf-or-n nil))
  ([src buf-or-n xform]
   (stream-chan!! src buf-or-n xform nil))
  ([^Stream src buf-or-n xform ex-handler]
   (doto (chan buf-or-n xform ex-handler)
     (onto-chan!!
       (clambda/stream-reducible src)))))

#_(defn stream-chan!
  "Returns a channel that will receive all the
   elements in Stream <src> (via `onto-chan!`)
   transformed per <xform>, and then close (also closing <src>)."
  ([src]
   (stream-chan! src nil))
  ([src buf-or-n]
   (stream-chan! src buf-or-n nil))
  ([src buf-or-n xform]
   (stream-chan! src buf-or-n xform nil))
  ([^Stream src buf-or-n xform ex-handler]
   (doto (chan buf-or-n xform ex-handler)
     (onto-chan!
       (clambda/stream-reducible src)))))

;; 3 variants of `line-chan` (LazySeq/Stream/Reducible based)
(defn line-seq-chan
  "Returns a channel that will receive all the lines
   in <src> (via `line-seq`) transformed per <xform>,
   and then close."
  ([src]
   (line-seq-chan src nil))
  ([src buf-or-n]
   (line-seq-chan src buf-or-n nil))
  ([src buf-or-n xform]
   (line-seq-chan src buf-or-n xform nil))
  ([src buf-or-n xform ex-handler]
   (let [^BufferedReader rdr (io/reader src)]
     (doto (chan buf-or-n xform ex-handler)
       (onto-chan!!
         (line-seq rdr) ;; LazySeq based
         (fn [ch]
           (ca/close! ch)
           (.close rdr))))))) ;; don't forget the Reader!

(defn line-stream-chan
  "Returns a channel that will receive all the lines
   in Path <src> (via `Files/lines`) transformed per <xform>,
   and then close. Functionally the same as `line-seq-chan`."
  ([src]
   (line-stream-chan src nil))
  ([src buf-or-n]
   (line-stream-chan src buf-or-n nil))
  ([src buf-or-n xform]
   (line-stream-chan src buf-or-n xform nil))
  ([^Path src buf-or-n xform ex-handler]
   (-> (Files/lines src)  ;; Stream based
       (stream-chan!! buf-or-n xform ex-handler))))

(defn lines-reducible-chan
  "Returns a channel that will receive all the lines
   in <src> (per `io/reader`) transformed per <xform>,
   and then close. Functionally the same as `line-seq-chan`."
  ([src]
   (lines-reducible-chan src nil))
  ([src buf-or-n]
   (lines-reducible-chan src buf-or-n nil))
  ([src buf-or-n xform]
   (lines-reducible-chan src buf-or-n xform nil))
  ([src buf-or-n xform ex-handler]
   (doto (chan buf-or-n xform ex-handler)
     (onto-chan!!  ;; Reducible based
       (clambda/lines-reducible (io/reader src))))))

(defn count-chan
  "Returns a channel that will (eventually) receive the
   total number of elements taken from <ch>."
  [ch]
  (ca/go-loop [n 0]
    (if-some [_ (ca/<! ch)]
      (recur (inc n))
      n)))

(comment
  ;; process the non-empty lines from <src> with <f> (one-by-one)
  ;; and report number of errors VS successes
  (->> (lines-reducible-chan src 1024 (comp (remove empty?) (keep f)) identity)
       (ca/split ut/throwable?)
       (map count-chan)
       (map ca/<!!) ;; => (0 120000)
       )

  )

(defn- round [n]
  (Math/round (double n)))

(defmacro pipe1
  "Pipes an element from the <from> channel and supplies it to the <to>
   channel. The to channel will be closed when the from channel closes.
   Must be called within a go block."
  [from to]
  `(if-some [v# (ca/<! ~from)]
     (ca/>! ~to v#)
     (ca/close! ~to)))

(defn- chan-throttler* [rate-ms bucket-size]
  (let [sleep-time (round (max (/ rate-ms) 10))
        token-value (long (round (* sleep-time rate-ms)))   ; how many messages to pipe per token
        bucket (chan [:dropping bucket-size])] ; we model the bucket with a buffered channel

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
      (let [tc (chan)] ; the throttled chan
        (ca/go
          (while (ca/<! bucket) ; park for a token
            (dotimes [_ (long token-value)]
              (when-not (pipe1 c tc)
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


(extend-protocol proto/IEmpty
  FixedBuffer
  (clone-empty [b]  (ca/buffer (.n b)))
  DroppingBuffer
  (clone-empty [b] (ca/dropping-buffer (.n b)))
  SlidingBuffer
  (clone-empty [b] (ca/sliding-buffer (.n b)))
  PromiseBuffer
  (clone-empty [_] (ca-buffers/promise-buffer))
  )

(defn empty-buffer
  "Returns a new/empty buffer of the same type and (buffering) capacity
   as the provided channel's buffer."
  [ch]
  (proto/clone-empty (ut/get-channel-buffer ch)))


