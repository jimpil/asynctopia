(ns asynctopia.core
  (:require [clojure.core.async :as ca]
            [asynctopia
             [protocols :as proto]
             [ops :as ops]
             [util :as ut]
             [channels :as channels]
             [null :as null]]
            [clojure.core.async.impl.mutex :as mutex])
  (:import (clojure.core.async.impl.channels ManyToManyChannel)
           (java.util LinkedList)))

(defn channel-buffer
  "Returns this channel's buffer. This is a non thread-safe
   mutable object, so be careful what you do with it."
  [^ManyToManyChannel c]
  (.buf c))

(defn clone-buffer
  "Returns a new/empty version of this buffer."
  [b]
  (proto/clone-empty b))

(defn snapshot-buffer
  "Returns a seq with the (current) contents this buffer."
  [b]
  (proto/snapshot b))

(defn clone-channel
  "Returns a new/empty version of this channel."
  ^ManyToManyChannel [^ManyToManyChannel ch]
  (ManyToManyChannel.
    (LinkedList.)
    (LinkedList.)
    (-> ch channel-buffer clone-buffer)
    (atom false)
    (mutex/mutex)
    (.-add_BANG_ ch)))

(def snapshot-channel
  "Returns a seq with the (current) contents of
   this channel's buffer (per `snapshot-buffer`)."
  (comp snapshot-buffer channel-buffer))

(defn consuming-with
  "Sets up two `go` loops for consuming values VS errors
   (identified by the <error?> predicate) from the <from> channel.
   Values are consumed via <consume!>, whereas errors via <error!>.
   The ideal consuming fns would submit to some thread-pool, send-off
   to some agent, or something along these lines (i.e. non-blocking).
   Both `go` loops gracefully terminate when <from> is closed."
  [consume! from & {:keys [error? error! to-error buffer]
                    :or {error? ut/throwable?
                         to-error identity
                         buffer 1024}}]
  (let [[errors-chan values-chan]
        (->> (ops/pipe-with consume! from
                            :to-error to-error
                            :error! error!
                            :buffer buffer)
             (ca/split error?))]
    ;; main consuming loop
    (ops/drain values-chan)
    ;; error handling loop (if a nested error occurs swallow it)
    (ops/sink-with error! errors-chan identity)))

(defn mix-with
  "Creates a `mix` against <out-chan>, adds all <in-chans> to it,
   and starts sinking <out-chan> with <f>. You need to close
   <out-chan> in order to stop the mixing/sinking loops (closing
   <in-chans> or unmixing-all won't suffice). Returns the `mix` object."
  ([f out-chan in-chans]
   (mix-with f out-chan in-chans ut/println-error-handler))
  ([f out-chan in-chans error!]
   (let [mixer (ca/mix out-chan)]
     (doseq [in in-chans] (ca/admix mixer in))
     (ops/sink-with f out-chan error!)
     mixer)))

(defn pkeep
  "Parallel `(keep f)` across <input> (collection, reducible or channel),
   handling errors with <error!>. <in-flight> controls parallelism
   (per `pipeline-blocking`).  <blocking-input?> controls how to turn
   <coll> into an input channel (`to-chan!` VS `to-chan!!`), whereas `buffer`
   controls how the output channel will be buffered (defaults to 1024).
   Returns a channel containing the single (collection) result
   (i.e. take a single element from it). The aforementioned collection
   may actually be smaller than <coll> (per `keep` semantics)."
  [f input & {:keys [error! in-flight buffer blocking-input?]
              :or {in-flight 1
                   buffer 1024
                   error! ut/println-error-handler}}]
  (let [channel-input? (ut/chan? input)
        to-chan* (if blocking-input? ca/to-chan!! ca/to-chan!)
        in-chan  (if channel-input?
                   input
                   (if (ut/reducible? input)
                     (doto (channels/chan 128)
                       (channels/onto-chan!! input))
                     (to-chan* input)))
        out-chan (channels/chan buffer)]
    (ca/pipeline-blocking in-flight
                          out-chan
                          (keep f)
                          in-chan
                          true
                          error!)
    (ca/into [] out-chan)))

(defmacro with-timeout
  "Returns a channel which will (eventually) receive
   either the result of <body>, or <timeout-val>."
  [ms timeout-val & body]
  `(let [ret-chan# (ca/promise-chan)]
     (ca/go (ops/>!? ret-chan# (do ~@body)))
     (ca/go
       (let [[x#] (ca/alts! [(ca/timeout ~ms) ret-chan#])]
         (if (nil? x#)
           ~timeout-val
           (null/restoring x#))))))

(defn with-counting
  "Returns a vector of two channels. The first will receive items per <ch>,
   whereas the second will (eventually) receive the total number of elements
   passed through (see `channels/count-chan`)."
  [ch]
  (let [multiple (ca/mult ch)]
    [(ca/tap multiple (channels/chan))
     (-> (ca/tap multiple (channels/chan))
         channels/count-chan)]))

(defn merge-reduce
  "If no <chans> are provided, essentially a wrapper to `ca/reduce`,
   otherwise merges all <chans> and reduces them with <f>, <init>."
  [f init chan & chans]
  (->> (if (seq chans)
         (ca/merge (cons chan chans))
         chan)
       (ca/reduce (comp f null/restoring) init)))


(defn thread-and ;; adapted from https://stackoverflow.com/questions/17621344/with-clojure-threading-long-running-processes-and-comparing-their-returns
  "Calls each of the no-arg <fns> on a separate thread (via `future`).
   Returns logical conjunction of the results.
   Short-circuit (and cancel the calls to remaining fns)
   on first falsey value returned."
  [& fns]
  (loop [futs-and-cs (doall
                       (for [f fns]
                         (let [c (ca/promise-chan)]
                           [(future (ca/>!! c (f))) c])))]
    (if (seq futs-and-cs)
      (let [[result c] (ca/alts!! (map second futs-and-cs))]
        (if result
          (recur (remove #(= c (second %)) futs-and-cs))
          (boolean (run! (comp future-cancel first) futs-and-cs))))
      true)))

(defn thread-or ;; adapted from https://stackoverflow.com/questions/16868252/short-circuiting-futures-in-clojure
  "Calls each of the no-arg <fns> on a separate thread (via `future`).
   Returns logical disjunction of the results.
   Short-circuits (and cancel the calls to remaining fns)
   on first truthy value returned."
  [& fns]
  (loop [futs-and-cs (doall
                       (for [f fns]
                         (let [c (ca/promise-chan)]
                           [(future (ca/>!! c (f))) c])))]
    (let [[result c] (ca/alts!! (map second futs-and-cs))]
      (if result
        (do (run! (comp future-cancel first) futs-and-cs)
            result)
        (let [new-futs-and-cs (remove #(= c (second %)) futs-and-cs)]
          (if (next new-futs-and-cs)
            (recur new-futs-and-cs)
            (ca/<!! (second (first new-futs-and-cs)))))))))
