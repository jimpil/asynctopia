(ns asynctopia.kafka.admin
  (:require [clojure.string :as str]
            [clojure.walk :as walk])
  (:import (java.util Map Collection Optional)))

(try
  (import [org.apache.kafka.clients.admin AdminClient NewTopic])
  (catch Exception _
    (throw
      (IllegalStateException.
        "`kafka` dependency not found - aborting ..."))))


(defn ^org.apache.kafka.clients.admin.AdminClient admin-client
  ([]
   (admin-client nil))
  ([options]
   (admin-client "localhost:9092" "local-admin" options))
  ([servers client-id options]
   (let [servers-str (if (string? servers) servers (str/join \, servers))
         ^Map opts (-> {:bootstrap.servers servers-str
                        :client.id         client-id}
                       (merge options)
                       walk/stringify-keys)]
     (org.apache.kafka.clients.admin.AdminClient/create opts))))

(defn ->new-topic
  [^String topic]
  (org.apache.kafka.clients.admin.NewTopic. topic (Optional/empty) (Optional/empty)))

(defn create-topics
  ([client topics]
   (create-topics client topics nil))
  ([^org.apache.kafka.clients.admin.AdminClient client
    topics
    create-options]
   (let [new-topics (mapv ->new-topic topics)]
     (if (nil? create-options)
       (.createTopics client ^Collection new-topics)
       (.createTopics client ^Collection new-topics create-options)))))


(defn delete-topics
  ([client topics]
   (delete-topics client topics nil))
  ([^org.apache.kafka.clients.admin.AdminClient client
    topics
    delete-options]
   (if (nil? delete-options)
     (.deleteTopics client ^Collection topics)
     (.deleteTopics client ^Collection topics delete-options))))
