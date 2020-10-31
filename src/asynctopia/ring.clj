(ns asynctopia.ring
  (:require [clojure.core.async :as ca]
            [clojure.java.io :as io]
            [asynctopia.ops :as ops]
            [asynctopia.util :as ut]
            [asynctopia.channels :as channels])
  (:import (clojure.lang IObj)
           (clojure.core.async.impl.channels ManyToManyChannel)))

(defonce timeout-response {:status 408})

(defn as-http-ret
  [x http-code]
  (when (instance? IObj x)
    (vary-meta x assoc :http-code http-code)))

(defn wrap-sync
  "Ring middleware which turns an asynchronous
   handler into a synchronous one (using promises).

   - timeout-ms: how long to wait for the <async-handler> - defaults to 10 seconds
   - timeout-val: what to return in case <timeout-ms> elapses - defaults to `{:status 408}`
   - error!: how to handle errors - function of 3 args (request/respond-fn/exception)
   - deliver-error?: when <error!> not provided delivers the exception's message - defaults to true"
  [async-handler {:keys [timeout-ms error!]
                  :or {error! #(throw %)
                       timeout-ms 10000}}]
  (assert (pos-int? timeout-ms) "Invalid <timeout-ms> ...")
  (fn sync-handler* [request]
    (let [p (promise)
          respond (fn [response]
                    (->> {:status (-> response meta (:http-code 200))
                          :body response}
                         (deliver p )))
          raise (bound-fn [^Throwable t]
                  (if (fn? error!)
                    (try (error! request respond t)
                         (catch Throwable _
                           (->> {:status 500
                                 :body "system-malfunction"}
                                (deliver p))))
                    (->> {:status (-> t ex-data (:http-code 500))
                          :body (.getMessage t)}
                         (deliver p))))]
      ;; fire the async handler
      (async-handler request respond raise)
      ;; and wait
      (deref p timeout-ms timeout-response)))
  )

(extend-type ManyToManyChannel
  ring.core.protocols/StreamableResponseBody
  (write-body-to-stream [this response output-stream]
    (let [out (io/output-stream output-stream)]
      (ops/sink-with!
        (fn [^bytes chunk]
          (.write out chunk)
          (.flush out))
        this
        (fn [_]
          (ca/close! this)
          (.close out))   ;; server async-timeout may kick-in
        #(.close out))))  ;; writing finished gracefully (channel was closed externally)
  )


(defn wrap-with-channel-body
  [async-handler  {:keys [timeout-ms error!]
                   :or {timeout-ms 10000
                        error! ut/println-error-handler}}]

  (fn async-handler*
    [request]
    (let [ch (ca/chan)
          respond (fn [response]
                    (ca/put! ch response)
                    (ca/close! ch))
          raise (fn [t]
                  (error! t)
                  (ca/close! ch))]
      ;; async-handler is expected to call respond once
      (async-handler request respond raise)
      {:status 200
       :body ch}
      )
    )
  )

(defn wrap-with-streaming-body
  [go-handler  {:keys [buffer error!]
                :or {error! ut/println-error-handler}}]

  (fn streaming-handler*
    [request]
    (let [ch (channels/chan buffer)
          raise (fn [t]
                  (error! t)
                  (ca/close! ch))]
      ;; go-handler is responsible for writing repeatedly
      ;; AND closing ch (presumably in a `go-loop`)
      (go-handler request ch raise)
      {:status 200
       :body ch}
      )
    )
  )
