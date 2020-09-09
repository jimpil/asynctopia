(ns asynctopia.ops
  (:require [clojure.core.async :as ca]))

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
  [c f]
  `(ca/go-loop []
     (when-some [x# (ca/<! ~c)]
       (~f x#)
       (recur))))

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
