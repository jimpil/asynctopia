(ns asynctopia.pubsub
  (:require [clojure.core.async :as ca]
            [asynctopia
             [core :as c]
             [channels :as channels]
             [util :as ut]]))
;; a `pub` is a go block pulling from one channel and feeding it in to
;; a `mult` (one per sub'ed topic), and a `mult` is a go block pulling
;; from one channel and writing to multiple channels.

(defn pub
  "Lile `ca/pub`, but creates mult-channels via `channels/chan`."
  ([ch topic-fn] (pub ch topic-fn (constantly nil)))
  ([ch topic-fn buf-fn]
   (let [mults (atom {}) ;;topic->mult
         ensure-mult
         (fn [topic]
           (or (get @mults topic)
               (get (swap! mults
                           #(if (% topic)
                              % ;; below is the (only) change
                              (assoc % topic (ca/mult (channels/chan (buf-fn topic))))))
                    topic)))
         p (reify
             ca/Mux
             (muxch* [_] ch)
             ca/Pub
             (sub* [p topic ch close?]
               (let [m (ensure-mult topic)]
                 (ca/tap m ch close?)))
             (unsub* [p topic ch]
               (when-let [m (get @mults topic)]
                 (ca/untap m ch)))
             (unsub-all* [_] (reset! mults {}))
             (unsub-all* [_ topic] (swap! mults dissoc topic)))]
     (ca/go-loop []
       (let [val (ca/<! ch)]
         (if (nil? val)
           (doseq [m (vals @mults)]
             (ca/close! (ca/muxch* m)))
           (let [topic (topic-fn val)
                 m (get @mults topic)]
             (when m
               (when-not (ca/>! (ca/muxch* m) val)
                 (swap! mults dissoc topic)))
             (recur)))))
     p)))


(defn pub-sub!
  "Configuration-driven (<topics>) `pub-sub` infrastructure.
   Creates a publication against channel <in> with <topic-fn> and
   <topic->buffer> (maps topic-keys to custom buffers - see `pub`).
   Then sets up subscriptions against the publication for all topics.
   <topics> must be either a map from topic to its process-fn (1-arg),
   or a vector of two elements - the list of topics followed by a common
   process-fn (2-arg, the topic-key first). Finally, and since we know
   the processing-fn per topic, sets-up consuming loops (again buffered per <topic->buffer>)
   for all subscriptions. The number of consuming loops per subscription is controlled
   by <topic->nconsumers> (a map from topic-key => positive integer, defaults to 1).
   Returns a vector of 2 elements - the publication, and the subscription channels.
   This can form the basis of a simple (in-process) event-bus (events arrive in <in>,
   and distributed to their respective topic processors)."
  [in topic-fn topics & {:keys [error? error! to-error topic->buffer topic->nconsumers]
                         :or   {error?            ut/throwable?
                                error!            ut/println-error-handler
                                to-error          identity
                                topic->nconsumers {}}}]
  (let [topic->processor (if (sequential? topics)
                           (let [[topics processor] topics]
                             (->> topics
                                  (map #(partial processor %))
                                  (zipmap topics)))
                           topics)
        topic-keys (keys topic->processor)
        topic->buffer (or topic->buffer
                          (zipmap topic-keys (repeat 1024))) ;; default buffer
        pb (pub in topic-fn topic->buffer) ;; create the publication
        sub-chans (map
                    (fn [[topic process-topic]]
                      (let [nconsumers (get topic->nconsumers topic 1) ;; default to single consumer
                            topic-buffer (topic->buffer topic)
                            [sub-buffer per-consumer]
                            (cond
                              (number? topic-buffer)
                              [topic-buffer (/ topic-buffer nconsumers)]

                              (sequential? topic->buffer) ;; [:dropping/:sliding N]
                              [topic-buffer (/ (second topic->buffer) nconsumers)]
                              ;; effectively 1 when topic-buffers are passed prebuilt
                              :else [nconsumers 1])

                            sub-chan (channels/chan sub-buffer)]
                        (ca/sub pb topic sub-chan) ;; subscribe
                        (dotimes [_ nconsumers]
                          (c/consuming-with ;; consume
                            process-topic
                            sub-chan
                            :buffer per-consumer
                            :error? error?
                            :error! error!
                            :to-error to-error))
                        sub-chan))
                    topic->processor)]
    [pb in (doall sub-chans)]))

(defn close-pub!
  "Unsubscribes everything from <pb> (a publication),
   after closing <pb-in-chan> (its input channel)."
  [pb pb-in-chan & _]
  (ca/close! pb-in-chan)
  (ca/unsub-all pb))


(comment
  (defn gen-val! [t]
    {:topic t
     :foo (rand-int 2000)})
  (defn dummy-processor
    [topic {:keys [foo]}]
    (if (even? foo)
      (println topic \: foo)
      (throw (ex-info "problem" {}))))

  (def in-chan (channels/chan))
  (pub-sub! in-chan :topic [[:t1 :t2] dummy-processor])
  (ca/>!! in-chan (gen-val! (rand-nth [:t1 :t2]))) ;; => true
  (ca/close! in-chan)
  )
