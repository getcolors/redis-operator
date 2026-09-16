(ns io.github.getcolors.redis-operator.tools
  "Rendering and the two transports the launcher uses: kubectl against one
  explicit context, and the DigitalOcean API for the recovery drill. Nothing
  here prints a payload that could carry a secret; error text from kubectl is
  passed through only for calls whose input holds none."
  (:require [babashka.process :as p]
            [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [green.cli :as green-cli]
            [io.github.getcolors.redis-operator.validate :as validate])
  (:import [java.time Instant]
           [java.util.concurrent TimeUnit]))

(def contract 1)

(def controller-name "colors-redis-operator")
(def credentials-secret "redis-credentials")
(def crd-name "redisdeployments.colors.getcolors.ai")
(def resource-type "redisdeployments.colors.getcolors.ai")
(def finalizer "colors.getcolors.ai/infrastructure")

(defn log [& parts]
  (locking *out*
    (println (str "redis-operator: " (str/join " " (map str parts))))
    (flush)))

(defn now [] (str (Instant/now)))

(defn instant-after? [later earlier]
  (try (.isAfter (Instant/parse later) (Instant/parse earlier))
       (catch Exception _ false)))

;; ------------------------------------------------------------------ rendering

(defn crd []
  (let [resource (io/resource "io/github/getcolors/redis_operator/crd.yml")]
    (when-not resource (throw (ex-info "CRD resource missing from the library classpath" {})))
    (green-cli/read-state "crd.yml" (slurp resource))))

(defn manifests
  "The operator installation: Namespace, CRD, ServiceAccount, Role,
  RoleBinding, PVC and the controller Deployment.

  The controller runs as root because the PVC subPath mounted at /root/.ssh
  must be owned and read by the user the workflows run as; `runAsNonRoot` is
  therefore deliberately absent. The rest of the container security context
  costs nothing the toolchain needs: no binary in the image is setuid, and no
  capability is required to run ssh, OpenTofu, Ansible or the AWS CLI as the
  owner of every file on the volume.

  The grace period exceeds the sum of the package's own caps (Ansible 7200 s,
  OpenTofu plan 1800 s) so a SIGKILL cannot land inside an infrastructure
  stage and leave the compute journal locked."
  [opts]
  (let [namespace (:namespace opts) image (:image opts)
        metadata {:name controller-name :namespace namespace}
        labels {:app controller-name}]
    (when-not (re-matches validate/dns-label-re (str namespace))
      (throw (ex-info ":namespace must be a DNS label" {})))
    (when-not (re-matches validate/image-re (str image))
      (throw (ex-info ":image must be an immutable reference with a sha256 digest" {})))
    [{:apiVersion "v1" :kind "Namespace" :metadata {:name namespace}}
     (crd)
     {:apiVersion "v1" :kind "ServiceAccount" :metadata metadata}
     {:apiVersion "rbac.authorization.k8s.io/v1" :kind "Role" :metadata metadata
      :rules [{:apiGroups ["colors.getcolors.ai"]
               :resources ["redisdeployments" "redisdeployments/status" "redisdeployments/finalizers"]
               :verbs ["get" "list" "watch" "patch" "update"]}]}
     {:apiVersion "rbac.authorization.k8s.io/v1" :kind "RoleBinding" :metadata metadata
      :subjects [{:kind "ServiceAccount" :name controller-name :namespace namespace}]
      :roleRef {:apiGroup "rbac.authorization.k8s.io" :kind "Role" :name controller-name}}
     {:apiVersion "v1" :kind "PersistentVolumeClaim" :metadata metadata
      :spec {:accessModes ["ReadWriteOnce"] :resources {:requests {:storage "5Gi"}}}}
     {:apiVersion "apps/v1" :kind "Deployment" :metadata metadata
      :spec {:replicas 1 :strategy {:type "Recreate"} :selector {:matchLabels labels}
             :template
             {:metadata {:labels labels}
              :spec (cond-> {:serviceAccountName controller-name
                             :terminationGracePeriodSeconds 10800
                             :containers [{:name "controller" :image image :args [namespace]
                                           :envFrom [{:secretRef {:name credentials-secret}}]
                                           :resources {:requests {:cpu "100m" :memory "256Mi"}
                                                       :limits {:memory "2Gi"}}
                                           :securityContext {:allowPrivilegeEscalation false
                                                             :capabilities {:drop ["ALL"]}
                                                             :seccompProfile {:type "RuntimeDefault"}}
                                           :volumeMounts [{:name "state" :mountPath "/data"}
                                                          {:name "state" :mountPath "/root/.ssh" :subPath "ssh"}]}]
                             :volumes [{:name "state" :persistentVolumeClaim {:claimName controller-name}}]}
                      (:image-pull-secret opts)
                      (assoc :imagePullSecrets [{:name (:image-pull-secret opts)}]))}}}]))

(defn manifest-list [opts]
  {:apiVersion "v1" :kind "List" :items (manifests opts)})

(defn resource
  "The RedisDeployment. `spec.suspend` is deliberately absent: the CRD
  defaults it on creation and an apply must never reset a suspension."
  [opts]
  {:apiVersion "colors.getcolors.ai/v1alpha1" :kind "RedisDeployment"
   :metadata {:name (:resource-name opts) :namespace (:namespace opts)}
   :spec {:state "running"
          :deletionPolicy (:deletion-policy opts)
          :reconcileInterval (:reconcile-interval opts)
          :config (validate/config opts)}})

(defn- sorted [x]
  (walk/postwalk #(if (map? %) (into (sorted-map) %) %) x))

(defn pretty-json [x] (str (json/generate-string (sorted x) {:pretty true}) "\n"))

(defn operator-dir [opts] (green-cli/stage-dir opts "operator"))
(defn evidence-dir [opts] (green-cli/stage-dir opts "evidence"))

(defn write-file! [path content]
  (let [file (.getCanonicalFile (io/file path)) tmp (io/file (str file ".tmp"))]
    (io/make-parents file)
    (spit tmp content)
    (when-not (.renameTo tmp file)
      (throw (ex-info (str "could not write " file) {})))
    (str file)))

(defn render! [opts]
  (let [dir (operator-dir opts)]
    {:manifests (write-file! (io/file dir "manifests.json") (pretty-json (manifest-list opts)))
     :resource (write-file! (io/file dir "redis-deployment.json") (pretty-json (resource opts)))}))

(defn write-evidence! [opts name data]
  (write-file! (io/file (evidence-dir opts) (str name ".json")) (pretty-json data)))

;; ------------------------------------------------------------------ processes

(defn run-command
  "Run argv with `input` on stdin, capturing both streams, bounded by
  `timeout-ms`. Exit 124 on timeout after the process tree is destroyed."
  [args {:keys [input timeout-ms] :or {timeout-ms 120000}}]
  (try
    (let [proc (p/process {:cmd (mapv str args) :in (or input "") :out :string :err :string})]
      (if (.waitFor (:proc proc) (long timeout-ms) TimeUnit/MILLISECONDS)
        (let [{:keys [exit out err]} @proc] {:exit exit :out (str out) :err (str err)})
        (do (p/destroy-tree proc)
            (try (deref proc) (catch Exception _ nil))
            {:exit 124 :out "" :err (str "command timed out after " timeout-ms "ms")})))
    (catch Exception e {:exit 127 :out "" :err (or (ex-message e) (str (class e)))})))

(defn kubectl-args [opts args {:keys [request-timeout?] :or {request-timeout? true}}]
  (into (cond-> ["kubectl" "--context" (str (:kube-context opts))]
          request-timeout? (conj "--request-timeout=30s"))
        (map str args)))

(defn kubectl
  "Run one kubectl command against the configured context. Throws on a
  non-zero exit; the message carries kubectl's stderr unless `quiet?`, which
  callers set when the input or output could hold a secret. `:json?` parses
  stdout, `:not-found` is returned (instead of a throw) when kubectl reports
  the object missing."
  ([opts args] (kubectl opts args {}))
  ([opts args {:keys [input timeout-ms quiet? json? request-timeout? not-found]
               :or {timeout-ms 120000 request-timeout? true} :as options}]
   (let [argv (kubectl-args opts args {:request-timeout? request-timeout?})
         {:keys [exit out err]} (run-command argv {:input input :timeout-ms timeout-ms})
         label (str "kubectl " (str/join " " (take 2 (map str args))))]
     (cond
       (zero? exit) (if json? (when-not (str/blank? out) (json/parse-string out true)) out)
       (and (contains? options :not-found) (re-find #"(?i)\bnot ?found\b" (str err))) not-found
       :else (throw (ex-info (if quiet?
                               (str label " failed (exit " exit "; output suppressed)")
                               (str label " failed (exit " exit "): " (str/trim (str err))))
                             {:exit exit :operation (first args)}))))))

(defn apply! [opts document & [{:keys [quiet?]}]]
  (kubectl opts ["apply" "-f" "-"] {:input (json/generate-string document) :quiet? quiet?}))

(defn get-resource [opts]
  (kubectl opts ["get" resource-type (:resource-name opts) "-n" (:namespace opts)
                 "--ignore-not-found" "-o" "json"] {:json? true}))

(defn patch-resource!
  "A JSON patch guarded by a resourceVersion test, so a concurrent edit fails
  the operation instead of being overwritten."
  [opts current patch]
  (kubectl opts ["patch" resource-type (:resource-name opts) "-n" (:namespace opts)
                 "--type=json" "-o" "json" "-p"
                 (json/generate-string
                  (into [{:op "test" :path "/metadata/resourceVersion"
                          :value (get-in current [:metadata :resourceVersion])}]
                        patch))]
           {:json? true}))

(defn probe
  "Run the controller image's probe inside the running pod and parse its
  JSON. The probe prints only non-secret evidence."
  [opts operation & args]
  (let [rehearse? (= "rehearse" operation)
        out (kubectl opts (into ["exec" (str "deployment/" controller-name) "-n" (:namespace opts)
                                 "--" "bb" "-m" "colors.probe" (:resource-name opts) (:namespace opts) operation]
                                args)
                     {:request-timeout? false :timeout-ms (if rehearse? 7800000 180000)})]
    (json/parse-string out true)))

(defn failures-dir [opts] (str "/data/work/" (:profile opts) "/failures"))

(defn failures
  "The failure logs the controller retained for this profile, listed inside
  the pod. Never their content: read one with
  `kubectl exec deployment/colors-redis-operator -- cat <path>`."
  [opts]
  (try
    (let [out (kubectl opts ["exec" (str "deployment/" controller-name) "-n" (:namespace opts)
                             "--" "sh" "-c" (str "ls -1 " (failures-dir opts) " 2>/dev/null || true")]
                       {:request-timeout? false :timeout-ms 60000})
          files (->> (str/split-lines (str out)) (map str/trim) (remove str/blank?) sort vec)]
      {:count (count files) :newest (last files)})
    (catch Exception e {:error (ex-message e)})))

(defn digitalocean
  "One DigitalOcean API call. A GET 404 is nil; every other error is reported
  by status only."
  [token method path]
  (let [http (requiring-resolve 'babashka.http-client/request)
        response (http {:method method :uri (str "https://api.digitalocean.com/v2/" path)
                        :headers {"Authorization" (str "Bearer " token) "Accept" "application/json"}
                        :timeout 45000 :throw false})
        status (:status response)]
    (cond
      (and (= 404 status) (= :get method)) nil
      (<= 200 status 299) (when-not (str/blank? (str (:body response)))
                            (json/parse-string (:body response) true))
      :else (throw (ex-info (str "DigitalOcean " (str/upper-case (name method)) " failed: HTTP " status)
                            {:status status})))))

;; ------------------------------------------------------------------ predicates

(defn ready?
  "Ready at the current generation: the controller has observed this spec and
  its Ready condition is True."
  [cr]
  (let [generation (get-in cr [:metadata :generation])
        status (:status cr)]
    (and (some? generation)
         (= generation (:observedGeneration status))
         (= "Ready" (:phase status))
         (boolean (some #(and (= "Ready" (:type %)) (= "True" (:status %)))
                        (:conditions status))))))

(defn suspended? [cr] (true? (get-in cr [:spec :suspend])))
(defn deleting? [cr] (some? (get-in cr [:metadata :deletionTimestamp])))

(defn acknowledged-suspension?
  "A suspension the controller has acknowledged at the current generation,
  which means the previous convergence finished."
  [cr]
  (and (suspended? cr) (not (deleting? cr))
       (= "Suspended" (get-in cr [:status :phase]))
       (= (get-in cr [:metadata :generation]) (get-in cr [:status :observedGeneration]))))

(defn phase-line [cr]
  (let [status (:status cr) condition (first (filter #(= "Ready" (:type %)) (:conditions status)))]
    (str "phase=" (or (:phase status) "none")
         " reason=" (or (:reason condition) "none")
         " generation=" (get-in cr [:metadata :generation])
         " observed=" (or (:observedGeneration status) "none")
         (when (suspended? cr) " suspended=true")
         (when (deleting? cr) " deleting=true"))))

(defn fatal [message] (ex-info message {::fatal true}))
(defn fatal? [e] (true? (::fatal (ex-data e))))

(defn owned-droplet!
  "Fail closed before any deletion: the live Droplet must be exactly the
  recorded ID and name, the deployment's profile, never a Kubernetes worker
  by ID or tag, and reachable at the recorded public address."
  [droplet probe profile workers]
  (let [id (str (:providerId probe))]
    (when-not (re-matches #"[0-9]+" id)
      (throw (fatal "Invalid Droplet ID")))
    (when-not (and droplet (= id (str (:id droplet))))
      (throw (fatal "Recorded provider ID does not match live Droplet")))
    (when-not (and (= profile (:profile probe)) (= profile (:name droplet)) (= profile (:name probe)))
      (throw (fatal "Droplet does not belong to the exact deployment profile")))
    (when (or (contains? (set (map str workers)) id)
              (some #(str/starts-with? (str %) "k8s:") (:tags droplet)))
      (throw (fatal "Refusing a Kubernetes worker")))
    (let [addresses (set (keep #(when (= "public" (:type %)) (:ip_address %))
                               (get-in droplet [:networks :v4])))]
      (when-not (contains? addresses (:ip probe))
        (throw (fatal "Recorded address differs from live Droplet"))))
    true))
