(ns asynctopia.server.embedded
  (:require [clojure.string :as str]
            [asynctopia.server.channel-body :as cb]
            [asynctopia.util :as ut]
            [clojure.core.async :as ca])
  (:import (org.apache.hc.core5.http2.impl.nio.bootstrap H2ServerBootstrap)
           (org.apache.hc.core5.http.protocol UriPatternMatcher HttpContext HttpCoreContext)
           (org.apache.hc.core5.http2 HttpVersionPolicy)
           (org.apache.hc.core5.reactor IOReactorConfig ListenerEndpoint)
           (java.util.concurrent TimeUnit)
           (org.apache.hc.core5.http.nio AsyncServerRequestHandler AsyncEntityProducer AsyncServerRequestHandler$ResponseTrigger)
           (org.apache.hc.core5.http.nio.support BasicRequestConsumer AsyncResponseBuilder)
           (org.apache.hc.core5.http EntityDetails Message HttpRequest HttpStatus Header EndpointDetails)
           (org.apache.hc.core5.http.nio.entity NoopEntityConsumer BasicAsyncEntityConsumer StringAsyncEntityConsumer)
           (org.apache.hc.core5.function Callback)
           (org.apache.hc.core5.http.impl.bootstrap HttpAsyncServer)
           (org.apache.hc.core5.io CloseMode)
           (java.net InetSocketAddress)
           (org.apache.hc.core5.util TimeValue)
           (java.lang System$LoggerFinder System$Logger$Level)
           (org.apache.hc.core5.concurrent FutureCallback)
           (org.apache.hc.core5.http.impl HttpProcessors)))

(defn io-reactor-config
  ^IOReactorConfig
  [{:keys [timeout timeout-unit io-threads tcp-no-delay]
    :or {timeout 20
         timeout-unit TimeUnit/SECONDS
         io-threads 4
         tcp-no-delay true}}]
  (-> (IOReactorConfig/custom)
      (.setSoTimeout timeout timeout-unit)
      (.setIoThreadCount io-threads)
      (.setTcpNoDelay tcp-no-delay)
      .build))

(defn- ring-req*
  [^Message msg ^HttpContext context]
  (let [endpoint-details (-> context HttpCoreContext/adapt .getEndpointDetails)
        ^HttpRequest request (.getHead msg)
        headers      (.getHeaders request)
        request-uri  (.getUri request)
        query-string (-> request-uri .getQuery)
        req-body (.getBody msg)]
    (cond-> {:server-port    (-> request-uri .getPort)
             :server-name    (-> request-uri .getHost)
             :remote-addr    (.getHostString ^InetSocketAddress
                               (.getRemoteAddress endpoint-details))
             :uri            (.getPath request-uri)
             :protocol       (str (.getVersion request))
             :scheme         (-> (.getScheme request)
                                 str/lower-case
                                 keyword)
             :request-method (-> (.getMethod request)
                                 str/lower-case
                                 keyword)
             :headers        (or (some->> (seq headers)
                                          (into {}
                                                (map (fn [^Header h]
                                                       [(str/lower-case (.getName h))
                                                        (str/lower-case (.getValue h))]))))
                                 {})}
            (some? query-string) (assoc :query-string  query-string)
            (some? req-body)     (assoc :body req-body))))

(defn- map->headers [m]
  (map
    (fn [[^String k ^String v]]
      (reify Header
        (isSensitive [_] true)
        (getName [_] (str/capitalize k))
        (getValue [_] v)))
    m))

(defn async-request-handler
  ^AsyncServerRequestHandler [handler]
  (reify AsyncServerRequestHandler
    (prepare [this request entity context]
      (case  (.getMethod ^HttpRequest request)
        ("GET", "DELETE", "HEAD")
        (BasicRequestConsumer. (NoopEntityConsumer.))
        ("POST", "PUT", "PATCH")
        (BasicRequestConsumer. (StringAsyncEntityConsumer.)))) ;; assuming text-based body (e.g JSON, EDN etc)
    (handle [this msg resp-trigger context]
      (let [{:keys [status headers body]
             :or {status 200}} (handler (ring-req* msg context))
            resp-builder (cond-> (AsyncResponseBuilder/create status)
                                 (ut/chan? body) (.setEntity (cb/->ChannelContentProducer body))
                                 (string? body)  (.setEntity ^String body)
                                 (seq headers)   (.setHeaders (into-array Header (map->headers headers)))
                                 true .build)]
        (.submitResponse
          ^AsyncServerRequestHandler$ResponseTrigger
          resp-trigger resp-builder context)))
    )
  )

(defn- register-routes
  ^H2ServerBootstrap
  [^H2ServerBootstrap server routes]
  (reduce-kv
    (fn [server ^String route-pattern handler]
      (.register
        server
        route-pattern
        (async-request-handler handler)))
    server
    routes))

(defn create-server
  ^HttpAsyncServer
  [{:keys [^String host-name ^String server-info routes]
    :or {host-name "local-async-server"}
    :as opts}]
  (-> (H2ServerBootstrap/bootstrap)
      (.setLookupRegistry (UriPatternMatcher.))
      (.setVersionPolicy HttpVersionPolicy/NEGOTIATE)
      (.setCanonicalHostName host-name)
      (.setIOReactorConfig (io-reactor-config opts))
      (.setExceptionCallback (reify Callback (execute [this t] (cb/log-error! t (class this)))))
      (.setHttpProcessor (HttpProcessors/server server-info))
      (register-routes routes)
      .create)
  )

(defn stop-listening!
  [^HttpAsyncServer s]
  (.close s CloseMode/GRACEFUL))

(defn- log-listener-endpoint!
  [^ListenerEndpoint le]
  (let [klass (class le)
        logger (-> (System$LoggerFinder/getLoggerFinder)
                   (.getLogger (.getName klass)
                               (.getModule klass)))]
    (when (.isLoggable logger System$Logger$Level/INFO)
      (.log logger System$Logger$Level/INFO
            (str "Async HTTP Server listening on: " (.getAddress le))))))

(def ^FutureCallback starting-listener-callback
  (reify FutureCallback
    (completed [_ endpoint]
      (log-listener-endpoint! endpoint))
    (failed [this ex]
      (cb/log-error! ex (class this)))
    (cancelled [_] nil)))

(defn start-listening!
  "Returns Future<ListenerEndpoint>."
  [^HttpAsyncServer s listen-port]
  (-> (Runtime/getRuntime)
      (.addShutdownHook (Thread. ^Runnable (partial stop-listening! s))))

  (.start s)
  (.listen s (InetSocketAddress. listen-port) starting-listener-callback))

(defn await-shutdown!
  "Blocks the calling thread for the provided number of days.
   Useful as the last call in -main methods to prevent the program from exiting."
  ([s]
   (await-shutdown! s Long/MAX_VALUE))
  ([^HttpAsyncServer s days]
   (.awaitShutdown s (TimeValue/ofDays days))))


(comment
  (def router
    {"/single" (fn single-response [request]
                (clojure.pprint/pprint request) ;; inspect the request at the REPL
                (let [ret (ca/promise-chan)] ;; promise-chan for single response
                  (future (Thread/sleep (rand-int 2000))
                          (ca/put! ret "YES\n"))
                  {:headers {"Content-Type" "text/plain"}
                   :body ret}))
     "/stream" (fn streaming-response [request]
                 (let [ret (ca/chan)] ;; regular chan for streaming response
                   (ca/go-loop [n 5]
                     (if (pos? n)
                       (do (ca/>! ret "YES\n")
                           (ca/<! (ca/timeout (rand-int 1000)))
                           (recur (dec n)))
                       (ca/close! ret)))
                   {:headers {"Content-Type" "text/plain"}
                    :body ret}))})

  (def server
    (doto (create-server {:routes router})
      (start-listening! 8080)))

  ;; curl http://localhost:8080/single
  ;; curl http://localhost:8080/stream

  (stop-listening! server)

  )