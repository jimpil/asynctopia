(ns asynctopia.correlator
  "A generic protocol+impl for correlating messages (e.g. request/response).
   Started out as a Kafka helper, but quickly realised there is nothing Kafka-specific here."
  (:require [clojure.core.cache.wrapped :as cw]))

(defprotocol ICorrelator
  "Correlator abstraction.
   - register-id!: Expects an <id>, and returns a promise (or whatever <exists!> returns)
   - deliver-id! : Expects an <id>, and returns true, or whatever <not-found!> returns"
  (register-id! [this id]   ;; adds a new promise and returns it (client blocks on it waiting)
                [this id exists!])
  (deliver-id!  [this id v] ;; finds the promise, delivers `v` to it (unblocks client), and removes it
                [this id v not-found!])
  (clear!       [this]))    ;; clears everything

(deftype Correlator [cache-atom]
  ICorrelator

  (register-id! [this cid]
    (register-id! this cid
      #(throw
         (IllegalStateException.
           (format "Correlation ID [%s] is already in-flight!" %)))))
  (register-id! [_ cid exists!]
    (let [exists? (volatile! true)
          p (cw/lookup-or-miss cache-atom cid
              (fn [_]
                (vreset! exists? false)
                (promise)))]
      (if @exists?
        (exists! cid)
        p)))

  (deliver-id! [this cid v]
    (deliver-id! this cid v
      #(throw
         (IllegalStateException.
           (format "Correlation ID [%s] is not in-flight!" %)))))
  (deliver-id! [_ cid v not-found!]
    (if-let [p (cw/lookup cache-atom cid)]
      (do (deliver p v)
          (cw/evict cache-atom cid)
          true)
      (not-found! cid)))

  (clear! [_]
    (swap! cache-atom empty))

  )

(defn id-correlator-ttl
  "Constructor function for a `Correlator` using a TTL cache
   under the covers. This ensures that keys are evicted
   regardless of whether their values were delivered (or not),
   thus eliminating memory leaks."
  ([]
   (id-correlator-ttl 10000))
  ([ttl]
   (id-correlator-ttl {} ttl))
  ([pending ttl]
   (Correlator.
     (cw/ttl-cache-factory pending :ttl ttl))))
