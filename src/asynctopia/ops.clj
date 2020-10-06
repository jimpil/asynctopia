(ns asynctopia.ops
  (:require [clojure.core.async :as ca]
            [asynctopia
             [buffers :as buffers]
             [null :as null]
             [util :as ut]]))

(defn pipe-with
  "Pipes the <from> channel into a newly created output-channel,
   returning the latter. Errors thrown by <f> are handled with `error!`
   (defaults to simply printing the error message)."
  [f from & {:keys [error! buffer]
             :or {error! ut/println-error-handler
                  buffer 1024}}]
  (->> (buffers/chan* buffer (map (comp null/replacing f)) error!)
       (ca/pipe from))) ;; returns the `to` channel (2nd arg)

(defn sink-with
  "Generic sinking `go-loop`. Fully consumes
   channel <ch>, passing the taken values through
   <f> (presumably side-effecting, ideally non-blocking).
   Errors thrown by <f> are handled with <error!>
   (defaults to simply printing the error message)."
  ([f ch]
   (sink-with f ch ut/println-error-handler))
  ([f ch error!]
   (ca/go-loop []
     (when-some [x (ca/<! ch)]
       (try (f (null/restoring x))
            (catch Throwable t (error! t)))
       (recur)))))

(def drain
  "Fully consumes a channel disregarding its contents.
   Useful against a piped `to` channel that uses a
   transducer (see `core/consuming-with`)."
  (partial sink-with identity))

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
     (sink-with f out-chan error!)
     mixer)))

(defmacro <!?deliver
  "Takes from channel <ch> and delivers the value to promise <p>.
   If the value is ::nil delivers nil."
  [ch p]
  `(deliver ~p (null/restoring (ca/<! ~ch))))

(defmacro >!?
  "Nil-safe variant of `>!`.
   If <x> is nil, will put ::nil.
   Must be called withing a `go` block."
  [ch x]
  `(ca/>! ~ch (null/replacing ~x)))

(defmacro >!!?
  "Nil-safe variant of `>!!`.
   If <x> is nil, will put ::nil."
  [ch x]
  `(ca/>!! ~ch (null/replacing ~x)))

(defn merge-reduce
  "If no <chans> are provided, essentially a wrapper to `ca/reduce`,
   otherwise merges all <chans> and reduces them with <f>, <init>."
  [f init chan & chans]
  (->> (if (seq chans)
         (ca/merge (cons chan chans))
         chan)
       (ca/reduce (comp f null/restoring) init)))
