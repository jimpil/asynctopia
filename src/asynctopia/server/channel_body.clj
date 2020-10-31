(ns asynctopia.server.channel-body
  (:require [clojure.core.async :as ca]
            [asynctopia.ops :as ops]
            [asynctopia.core :as c])
  (:import (clojure.core.async.impl.channels ManyToManyChannel)
           (org.apache.hc.core5.http.nio AsyncEntityProducer DataStreamChannel)
           (java.nio ByteBuffer CharBuffer)
           (java.lang System$LoggerFinder System$Logger$Level)
           (java.nio.charset StandardCharsets)))

(declare log-error!)

(defn- byte-buffer*
 ^ByteBuffer [x]
  (cond
    (bytes? x)
    (ByteBuffer/wrap x)

    (string? x)
    (->> (CharBuffer/wrap ^String x)
         (.encode StandardCharsets/UTF_8))
    :else
    (throw
      (IllegalStateException.
        "Can only deal with String or byte-array chunks!"))))

(deftype ChannelContentProducer
  [^ManyToManyChannel ch promise-chan? ^String content-type ^bytes delimiter state]
  AsyncEntityProducer
  (isRepeatable   [this] false)
  (isChunked      [this] true)
  (getContentType [this] content-type)
  (getContentLength [this] -1)
  (getTrailerNames [this] #{})
  (getContentEncoding [this]
    (when (not= content-type "application/octet-stream") "UTF-8"))
  (failed [this exception]
    (log-error! exception (class this))
    (.releaseResources this))
  (releaseResources [this]
    (ca/close! ch))
  (available [this]
    (if (true? promise-chan?) 1 Integer/MAX_VALUE))
  (^void produce [this ^DataStreamChannel ds]
    ;; we're not in control of how often this is called
    (when (compare-and-set! state :READY :ACTIVE)
      (if (true? promise-chan?)
        (ca/go
          (when-some [resp (ca/<! ch)]
            (->> resp byte-buffer* (.write ds))
            (reset! state :DONE)
            (.endStream ds)))

        (ops/sink-with!
          (fn [chunk]
            (->> chunk byte-buffer* (.write ds))
            (some->> delimiter ByteBuffer/wrap (.write ds)))
          ch
          #(do (reset! state :FAIL)
               (.endStream ds)
               (.failed this %))
          #(do (reset! state :DONE)
               (.endStream ds)))
        )
      )
    )
  )

(defn ->ChannelContentProducer
  ""
  ^ChannelContentProducer
  [ch & {:keys [content-type delimiter]
         :or {content-type "application/octet-stream"}}]
  (ChannelContentProducer.
    ch
    (c/promise-chan? ch)
    content-type
    (if (bytes? delimiter)
      delimiter
      (some-> delimiter str .getBytes))
    (atom :READY)))

(defn log-error!
  [^Throwable t ^Class klass]
  (let [logger (-> (System$LoggerFinder/getLoggerFinder)
                   (.getLogger (.getName klass)
                               (.getModule klass)))]
    (when (.isLoggable logger System$Logger$Level/ERROR)
      (.log logger System$Logger$Level/ERROR "Async content producer error:" t))))
