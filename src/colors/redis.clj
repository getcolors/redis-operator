(ns colors.redis
  "Redis package adapter. Provider absence, configuration drift and service health
  are independent observations; unknown state is never permission to recreate."
  (:require [babashka.http-client :as http]
            [cheshire.core :as json]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [green.process :as process]
            [green.workflow :as wf]
            [io.github.getcolors.compute-inspection :as inspection]
            [io.github.getcolors.redis.compute :as compute]
            [io.github.getcolors.redis.tools :as tools]
            [io.github.getcolors.redis.validate :as validation]
            [io.github.getcolors.redis.workflow :as redis])
  (:import [java.security MessageDigest]))

(def credential-vars
  {"COLORS_PAR_DO_TOKEN" :do-token
   "COLORS_PAR_R2_ACCESS_KEY_ID" :r2-access-key-id
   "COLORS_PAR_R2_SECRET_ACCESS_KEY" :r2-secret-access-key
   "COLORS_PAR_REDIS_BACKUP_R2_ACCESS_KEY_ID" :redis-backup-r2-access-key-id
   "COLORS_PAR_REDIS_BACKUP_R2_SECRET_ACCESS_KEY" :redis-backup-r2-secret-access-key})

(defn check-environment! [env]
  (when (some #(and (str/starts-with? % "COLORS_PAR_")
                    (not (contains? credential-vars %))) (keys env))
    (throw (ex-info "Unexpected COLORS_PAR environment override; desired configuration must come from the resource" {})))
  (when (some #(str/blank? (get env %)) (keys credential-vars))
    (throw (ex-info "Required operator credentials are missing" {}))))

(defn options [config]
  (merge redis/defaults (dissoc config :green.kubernetes/resource)
         (into {} (for [[env key] credential-vars] [key (System/getenv env)]))
         {:workdir (or (System/getenv "COLORS_WORKDIR") "/data/work")
          :compute-prevent-destroy true :redis-storage-managed false
          :provider-compute "digitalocean" :provider-backend "r2"}))

(defn identity [config]
  ["r2" (:r2-endpoint config) (:r2-bucket config) (:profile config)])

(defn validate [config]
  (concat
   (validation/state-errors (options config))
   (when-not (re-matches #"[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}" (str (:profile config)))
     ["Invalid profile"])
   (when-not (= "running" (get-in config [:green.kubernetes/resource :spec :state] "running"))
     ["This release supports running state only"])
   (when (contains? config :compute-prevent-destroy) ["Use spec.deletionPolicy"])))

(defn config-hash [config]
  (let [desired (dissoc config :green/event :green.kubernetes/resource)]
    (apply str (map #(format "%02x" (bit-and 255 %))
                    (.digest (MessageDigest/getInstance "SHA-256")
                             (.getBytes (pr-str (into (sorted-map) desired)) "UTF-8"))))))

(defn marker-path [config]
  (io/file (:workdir (options config)) (:profile config) "operator-success.edn"))

(defn read-marker [config]
  (let [path (marker-path config)]
    (when (.exists path) (edn/read-string (slurp path)))))

(defn write-marker! [config node]
  (let [path (marker-path config) tmp (io/file (str path ".tmp"))]
    (io/make-parents path)
    (spit tmp (pr-str {:config-hash (config-hash config) :provider-id (str (:provider_id node))}))
    (when-not (.renameTo tmp path) (throw (ex-info "Could not persist convergence result" {})))))

(defn inspect [opts]
  (inspection/read-deployment opts (tools/environment opts) {} (compute/requirements opts)))

(defn provider-get [opts node]
  (let [id (str (:provider_id node))]
    (when-not (re-matches #"[0-9]+" id) (throw (ex-info "Invalid recorded Droplet ID" {})))
    (let [response (http/get (str "https://api.digitalocean.com/v2/droplets/" id)
                             {:headers {"Authorization" (str "Bearer " (:do-token opts))}
                              :timeout 20000 :throw false})]
      (case (:status response)
        404 nil
        200 (let [droplet (:droplet (json/parse-string (:body response) true))]
              (when-not (and (= id (str (:id droplet))) (= (:name node) (:name droplet)))
                (throw (ex-info "Provider identity does not match owned state" {})))
              droplet)
        (throw (ex-info "DigitalOcean observation failed" {:status (:status response)}))))))

(def health-command
  "cd /opt/redis && docker compose exec -T redis sh -c 'export REDISCLI_AUTH=\"$(sed -n '\"'\"'s/^requirepass //p'\"'\"' /etc/redis/redis.conf)\"; redis-cli --no-auth-warning PING'")

(defn service-health [opts node]
  (let [result (process/run-with-timeout
                ["ssh" "-o" "BatchMode=yes" "-o" "ConnectTimeout=10"
                 "-o" "StrictHostKeyChecking=no" "-o" "UserKnownHostsFile=/dev/null"
                 "-i" (:ssh_identity_file node)
                 (str (:user node) "@" (:ip node)) health-command] {} 20000)]
    (and (zero? (:exit result)) (= "PONG" (str/trim (:out result))))))

(defn provider-matches? [opts node droplet]
  (and (= (:name node) (:name droplet))
       (= (:digitalocean-region opts) (get-in droplet [:region :slug]))
       (= (:digitalocean-size opts) (:size_slug droplet))
       (= (:digitalocean-image opts) (get-in droplet [:image :slug]))
       (some #(and (= "public" (:type %)) (= (:ip node) (:ip_address %)))
             (get-in droplet [:networks :v4]))))

(defn observe
  ([config] (observe config {}))
  ([config dependencies]
   (let [opts (options config)
         result ((get dependencies :inspect inspect) opts)]
     (case (:status result)
       ("absent" "destroyed") {:exists? false :matches? false :ready? false}
       "partial" {:exists? true :matches? false :ready? false}
       "present"
       (let [node (first (get-in result [:cluster :nodes]))
             droplet ((get dependencies :provider-get provider-get) opts node)]
         (if-not droplet
           ;; On deletion keep exists true while shared firewall/key/state remain.
           {:exists? (= :delete (:green/event config)) :matches? false :ready? false}
           (let [healthy (boolean (and (= "active" (:status droplet))
                                     ((get dependencies :service-health service-health) opts node)))
                 marker ((get dependencies :read-marker read-marker) config)
                 matches (and (= (config-hash config) (:config-hash marker))
                              (= (str (:provider_id node)) (:provider-id marker))
                              (provider-matches? opts node droplet) healthy)]
             {:exists? true :matches? (boolean matches) :ready? healthy})))
       (throw (ex-info "Owned infrastructure state could not be read" {}))))))

(defn converge [config]
  (let [result (wf/run redis/workflow (assoc (options config) :green/event :create))]
    (when-not (wf/failed? result)
      (let [state (inspect (options config)) node (first (get-in state [:cluster :nodes]))]
        (when-not (and (= "present" (:status state)) (:provider_id node))
          (throw (ex-info "Convergence finished without readable owned state" {})))
        (write-marker! config node)))
    ;; Do not return workflow opts, credentials or captured program output.
    {:green/exit (if (wf/failed? result) 1 0)}))

(defn delete [config]
  (when-not (= "Destroy" (get-in config [:green.kubernetes/resource :spec :deletionPolicy]))
    (throw (ex-info "Destroy must be explicitly selected" {})))
  (let [opts (assoc (options config) :green/event :delete :compute-prevent-destroy false)
        state (inspect opts)
        missing? (and (= "present" (:status state))
                      (nil? (provider-get opts (first (get-in state [:cluster :nodes])))))
        ;; A missing host cannot run the package cleanup play. The compute
        ;; workflow still destroys owned shared resources and retires state.
        result (if missing?
                 (wf/run (wf/workflow {:start :ssh-config
                                       :wire-fn (fn [step _]
                                                  (case step
                                                    :ssh-config [tools/ansible-local-step :compute]
                                                    :compute [tools/infrastructure-step]))})
                         (assoc opts :colors-compute/cluster (:cluster state)))
                 (wf/run redis/workflow opts))]
    {:green/exit (if (wf/failed? result) 1 0)}))

(defn package []
  {:resource {:group "colors.getcolors.ai" :version "v1alpha1"
              :plural "redisdeployments" :kind "RedisDeployment"}
   :validate validate :identity identity :observe observe :converge converge :delete delete})
