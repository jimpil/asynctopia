(ns asynctopia.pubsub
  (:require [clojure.core.async :as ca]
            [asynctopia
             [core :as c]
             [protocols :as proto]
             [channels :as channels]
             [util :as ut]]))

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
                                  (map
                                    (fn [topic]
                                      (partial processor topic)))
                                  (zipmap topics)))
                           topics)
        topic->buffer (or topic->buffer
                          (zipmap (keys topic->processor)
                                  (repeat 1024))) ;; default buffer
        sub-chans (repeatedly (count topic->processor) channels/chan)
        pb (ca/pub in topic-fn topic->buffer)] ;; create the publication

    (dorun
      (map
        (fn [[topic process-topic] c]
          (ca/sub pb topic c) ;; topic subscription
          (dotimes [_ (get topic->nconsumers topic 1)] ;; default to single consumer
            (c/consuming-with ;; topic consumption
              process-topic
              c
              :buffer (let [b (get topic->buffer topic)]
                        (if (number? b) b (proto/clone-empty b)))
              :error? error?
              :error! error!
              :to-error to-error)))
        topic->processor
        sub-chans))

    [pb sub-chans]))


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
