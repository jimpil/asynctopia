(defproject jimpil/asynctopia "0.2.4-SNAPSHOT"
  :description "High-level `core.async` utilities"
  :url "https://github.com/jimpil/asynctopia"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.1" :scope "provided"]
                 [org.clojure/core.async "1.3.610"]
                 [clambda "0.1.5"]]
  :profiles {:dev {:dependencies
                   [[org.apache.kafka/kafka_2.13 "2.6.0"]
                    [org.slf4j/slf4j-api "2.0.0-alpha1"]
                    [org.slf4j/slf4j-simple "2.0.0-alpha1"]]}}
  :repl-options {:init-ns asynctopia.core}

  :release-tasks [["vcs" "assert-committed"]
                  ["change" "version" "leiningen.release/bump-version" "release"]
                  ["vcs" "commit"]
                  ["vcs" "tag" "--no-sign"]
                  ["deploy"]
                  ["change" "version" "leiningen.release/bump-version"]
                  ["vcs" "commit"]
                  ;["vcs" "push"]
                  ]
  :deploy-repositories [["releases" :clojars]] ;; lein release :patch
  :signing {:gpg-key "jimpil1985@gmail.com"})
