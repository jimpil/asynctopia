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
  [f from & {:keys [error! to-error buffer]
             :or {error! ut/println-error-handler
                  to-error identity
                  buffer 1024}}]
  (->> (buffers/chan* buffer
                      (comp (map f)
                            (map to-error)
                            (map null/replacing))
                      error!)
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
   (sink-with f ch error! (constantly nil)))
  ([f ch error! done!]
   (ca/go-loop []
     (if-some [x (ca/<! ch)]
       (do (try (f (null/restoring x))
                (catch Throwable t (error! t)))
           (recur))
       (done!)))))

(def drain
  "Fully consumes a channel disregarding its contents.
   Useful against a piped `to` channel that uses a
   transducer (see `core/consuming-with`)."
  (partial sink-with identity))

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
