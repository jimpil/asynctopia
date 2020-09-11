(ns asynctopia.core
  (:require [clojure.core.async :as ca]
            [asynctopia
             [ops :as ops]
             [channels :as channels]
             [util :as ut]]))

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
                        :to-error to-error
                        :buffer buffer)
             (ca/split error?))]
    (ops/go-consume! values-chan identity) ;; main consuming loop
    (ops/go-consume! errors-chan error!))) ;; error handling loop


(defn pkeep
  "Parallel `(keep f)` across <coll>, handling errors with <error!>.
   <in-flight> controls parallelism (per `pipeline-blocking`).
   If <async?> is true (default), immediately returns a promise which will
   (eventually) be delivered with a vector of results, otherwise the vector is
   (synchronously) returned. The aforementioned vector may actually be smaller
   than <coll> (per `keep` semantics)."
  [f coll & {:keys [error! in-flight async?]
             :or {in-flight 1
                  async? true
                  error! ut/println-error-handler}}]
  (let [in-chan  (ca/to-chan! coll)
        out-chan (channels/chan)
        ret (when async? (promise))]
    (ca/pipeline-blocking in-flight
                          out-chan
                          (keep f)
                          in-chan
                          true
                          error!)
    (let [ret-chan (ca/into [] out-chan)]
      (if (nil? ret)
        (ca/<!! ret-chan)
        (do (ca/go (ops/<!?deliver ret-chan ret))
            ret)))))

(defmacro with-timeout
  "Returns a promise which will (eventually) receive
   either the result of <body>, or <timeout-val>."
  [ms timeout-val & body]
  `(let [ret-chan# (ca/promise-chan)
         ret# (promise)]
     (ca/go (ops/>!? ret-chan# (do ~@body)))
     (ca/go
       (let [[x#] (ca/alts! [(ca/timeout ~ms) ret-chan#])]
         (->> (if (nil? x#) ~timeout-val (ops/nil-restoring x#))
              (deliver ret#))))
     ret#))


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

