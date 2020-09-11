(ns asynctopia.ops
  (:require [clojure.core.async :as ca]
            [asynctopia.channels :as channels]))

(defn pipe-with
  "Pipes the <from> channel into a newly created output-channel
   (created with `(chan buffer (keep f) to-error)`), returning the latter."
  [f from & {:keys [to-error buffer]
             :or {to-error identity
                  buffer 1024}}]
  (->> (channels/chan buffer (keep f) to-error)
       (ca/pipe from))) ;; returns the `to` channel (2nd arg)

(defn sink-with
  "Generic sinking `go-loop`. Fully consumes
   channel <ch>, passing the taken values through
   <f> (presumably side-effecting, ideally non-blocking)."
  [f ch]
  (ca/go-loop []
    (when-some [x (ca/<! ch)]
      (f x)
      (recur))))

(def drain
  "Fully consumes a channel disregarding its contents.
   Useful against a piped `to` channel that uses a transducer."
  (partial sink-with identity))

(defmacro nil-converting
  "If <x> is nil returns ::nil.
   Useful when putting (unknown) stuff into channels."
  [x]
  `(if-some [x# ~x] x# ::nil))

(defmacro nil-restoring
  "If <x> is ::nil returns nil.
   Useful when taking (nil-safe) stuff from channels."
  [x]
  `(let [x# ~x]
     (when (not= ::nil x#) x#)))

(defmacro <!?
  "Nil-preserving variant of `<!`.
   If <x> is ::nil, will return nil.
   Must be called withing a `go` block."
  [c]
  `(nil-restoring (ca/<! ~c)))

(defmacro <!?deliver
  "Takes from channel <c> and delivers the value to promise <p>.
   If the value is ::nil delivers nil."
  [c p]
  `(deliver ~p (<!? ~c)))

(defmacro >!?
  "Nil-safe variant of `>!`.
   If <x> is nil, will put ::nil.
   Must be called withing a `go` block."
  [c x]
  `(ca/>! ~c (nil-converting ~x)))

(defn merge-reduce
  "If no <chans> are provided, essentially a wrapper to `ca/reduce`,
   otherwise merges all <chans> and reduces them with <f>, <init>."
  [f init chan & chans]
  (->> (if (seq chans)
         (ca/merge (cons chan chans))
         chan)
       (ca/reduce f init)))
