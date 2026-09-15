(ns colors.probe
  "Explicit development probes. Prints only selected nonsecret evidence."
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [colors.redis :as adapter]
            [green.process :as process]
            [green.workflow :as wf]
            [io.github.getcolors.redis.workflow :as redis]))

(defn safe-token [value]
  (when-not (and (string? value) (re-matches #"[A-Za-z0-9:_-]{1,160}" value))
    (throw (ex-info "Invalid probe token" {}))) value)

(defn remote [node command]
  (let [result (process/run-with-timeout
                ["ssh" "-o" "BatchMode=yes" "-o" "ConnectTimeout=10"
                 "-o" "StrictHostKeyChecking=no" "-o" "UserKnownHostsFile=/dev/null"
                 "-i" (:ssh_identity_file node) (str (:user node) "@" (:ip node)) command]
                {} 30000)]
    (when-not (zero? (:exit result)) (throw (ex-info "Remote probe failed" {})))
    (str/trim (:out result))))

(defn redis-command [node command]
  ;; The fixed health command reads the password inside the container. Only
  ;; validated probe tokens are appended; no password enters process argv.
  (remote node (str/replace adapter/health-command "PING'" (str "--raw " command "'"))))

(defn probe [resource namespace operation key value]
  (doseq [v [resource namespace]] (safe-token v))
  (let [result (process/run-with-timeout ["kubectl" "get" "redisdeployment" resource "-n" namespace "-o" "json"] {} 30000)
        _ (when-not (zero? (:exit result)) (throw (ex-info "Resource read failed" {})))
        cr (json/parse-string (:out result) true)
        config (assoc (get-in cr [:spec :config]) :profile
                      (or (get-in cr [:spec :config :profile]) (get-in cr [:status :profile]) (str namespace "--" resource)))
        opts (adapter/options config)
        state (adapter/inspect opts)
        _ (when-not (= "present" (:status state)) (throw (ex-info "State not ready" {})))
        node (first (get-in state [:cluster :nodes]))
        evidence {:providerId (str (:provider_id node)) :name (:name node) :ip (:ip node) :profile (:profile opts)
                  :convergenceRecordModifiedMs (.lastModified (adapter/marker-path config))}]
    (merge evidence
           (case operation
             "inspect" {}
             "health" {:healthy (= "PONG" (redis-command node "PING"))}
             "set-marker" (do (safe-token key) (safe-token value)
                              (when-not (= "OK" (redis-command node (str "SET " key " " value)))
                                (throw (ex-info "Marker write failed" {})))
                              {:marker (redis-command node (str "GET " key))})
             "get-marker" (do (safe-token key) {:marker (redis-command node (str "GET " key))})
             "rehearse" (let [result (binding [*out* (java.io.StringWriter.) *err* (java.io.StringWriter.)]
                                       (wf/run redis/workflow (assoc opts :green/event :rehearse)))]
                           {:rehearsalPassed (not (wf/failed? result))})
             (throw (ex-info "Unknown probe operation" {}))))))

(defn -main [& [resource namespace operation key value]]
  (try
    (println (json/generate-string (probe resource namespace operation key value)))
    (catch Exception _
      (binding [*out* *err*] (println "Probe failed; captured output suppressed"))
      (System/exit 1))))
