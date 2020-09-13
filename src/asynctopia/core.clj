(ns asynctopia.core
  (:require [clojure.core.async :as ca]
            [asynctopia
             [ops :as ops]
             [util :as ut]
             [channels :as channels]
             [null :as null]]))

(defn consuming-with
  "Sets up two `go` loops for consuming values VS errors
   (identified by the <error?> predicate) from the <from> channel.
   Values are consumed via <consume!>, whereas errors via <error!>.
   The ideal consuming fns would submit to some thread-pool, send-off
   to some agent, or something along these lines (i.e. non-blocking).
   Both `go` loops gracefully terminate when <from> is closed."
  [consume! from & {:keys [error? error! to-error buffer]
                    :or {error? ut/throwable?
                         error! ut/println-error-handler
                         to-error identity
                         buffer 1024}}]

  (let [[errors-chan values-chan]
        (->> (ops/pipe-with consume!
                            from
                            :error! to-error
                            :buffer buffer)
             (ca/split error?))]
    ;; main consuming loop
    (ops/drain values-chan)
    ;; error handling loop (if a nested error occurs swallow it)
    (ops/sink-with error! errors-chan identity)))


(defn pkeep
  "Parallel `(keep f)` across <coll> (collection or channel),
   handling errors with <error!>. <in-flight> controls parallelism
   (per `pipeline-blocking`).  <blocking-input?> controls how to turn
   <coll> into an input channel (`to-chan!` VS `to-chan!!`), whereas `buffer`
   controls how the output channel will be buffered (defaults to `(count coll)`).
   Returns a channel containing the single (collection) result
   (i.e. take a single element from it). The aforementioned collection
   may actually be smaller than <coll> (per `keep` semantics)."
  [f coll & {:keys [error! in-flight buffer blocking-input?]
             :or {in-flight 1
                  buffer 1024
                  error! ut/println-error-handler}}]
  (let [channel-input? (ut/chan? coll)
        to-chan* (if blocking-input? ca/to-chan!! ca/to-chan!)
        in-chan  (if channel-input? coll (to-chan* coll))
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

