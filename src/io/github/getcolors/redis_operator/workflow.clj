(ns io.github.getcolors.redis-operator.workflow
  "The launcher-side lifecycle: one workflow whose graph depends on the
  event. `build` renders; `create` installs the controller and converges one
  RedisDeployment; `delete` destroys through the controller's finalizer;
  `check`, `rehearse`, `drill` and `restart` are the operational verbs in
  the operator namespace."
  (:require [clojure.string :as str]
            [green.cli :as green-cli]
            [green.dry-run :as dry-run]
            [green.lifecycle :as lifecycle]
            [green.progress :as progress]
            [green.workflow :as wf]
            [io.github.getcolors.redis-operator.operator :as operator]
            [io.github.getcolors.redis-operator.tools :as tools]
            [io.github.getcolors.redis-operator.validate :as validate]))

(def defaults {:namespace "colors-redis" :reconcile-interval "60s" :deletion-policy "Retain"
               :compute-prevent-destroy true :provider-compute "digitalocean"
               :provider-backend "r2" :workdir ".colors"})

(def allowed-events [:build :create :check :rehearse :drill :restart :delete])

(defn start-step
  ([opts] (start-step opts (System/getenv)))
  ([opts env]
   (lifecycle/preflight
    (cond-> opts (validate/missing? (:resource-name opts)) (assoc :resource-name (:profile opts)))
    {:defaults defaults :overlay green-cli/read-pars
     :validators
     [(fn [_ env _] (validate/env-errors env))
      (fn [opts _ _] (validate/state-errors opts))
      (fn [_ env {:keys [event real?]}]
        (when (and real? (= :create event)) (validate/credential-errors env)))
      (fn [_ env {:keys [event]}]
        (when (= :drill event) (validate/drill-errors env)))
      (fn [_ env {:keys [event real?]}]
        (when (and real? (= :drill event) (str/blank? (get env "COLORS_PAR_DO_TOKEN")))
          ["required credential is not set: COLORS_PAR_DO_TOKEN"]))
      (fn [opts _ {:keys [event]}]
        (when (= :delete event) (validate/destroy-errors opts)))]}
    env)))

(defn render-step [opts]
  (let [{:keys [manifests resource]} (tools/render! opts)]
    (tools/log "rendered" manifests)
    (tools/log "rendered" resource)
    (assoc opts :green/exit 0)))

;; ---------------------------------------------------------------- create

(defn namespace-step [opts]
  (tools/apply! opts (first (tools/manifests opts)))
  (assoc opts :green/exit 0))

(defn credentials-step
  "The five credential variables, read from this process's environment and
  sent to kubectl on stdin. Output is suppressed: an apply error can echo
  the document."
  ([opts] (credentials-step opts (System/getenv)))
  ([opts env]
   (tools/apply! opts {:apiVersion "v1" :kind "Secret"
                       :metadata {:name tools/credentials-secret :namespace (:namespace opts)}
                       :type "Opaque"
                       :stringData (into {} (for [k validate/credentials] [k (get env k)]))}
                 {:quiet? true})
   (tools/log "applied Secret" (str (:namespace opts) "/" tools/credentials-secret))
   (assoc opts :green/exit 0)))

(defn pull-secret-step
  "When the deployment names a pull Secret it does not own (DOKS registry
  integration injects one into every namespace), wait up to two minutes for
  it. Only a not-found answer is worth waiting on; an auth or context error
  surfaces at once with kubectl's text."
  [opts]
  (if-let [name (:image-pull-secret opts)]
    (do
      (operator/wait-for
       {:label (str "pull Secret " (:namespace opts) "/" name) :timeout-ms 120000 :interval-ms 5000}
       (fn [] (try (tools/kubectl opts ["get" "secret" name "-n" (:namespace opts) "-o" "name"] {:not-found nil})
                   (catch Exception e (throw (tools/fatal (ex-message e)))))))
      (tools/log "pull Secret present" name)
      (assoc opts :green/exit 0))
    (assoc opts :green/exit 0)))

(defn install-step [opts]
  (tools/apply! opts (tools/manifest-list opts))
  (tools/kubectl opts ["wait" "--for=condition=Established" (str "crd/" tools/crd-name) "--timeout=60s"]
                 {:request-timeout? false :timeout-ms 90000})
  (tools/kubectl opts ["rollout" "status" (str "deployment/" tools/controller-name)
                       "-n" (:namespace opts) "--timeout=600s"]
                 {:request-timeout? false :timeout-ms 630000})
  (tools/log "controller rolled out" (str (:namespace opts) "/" tools/controller-name))
  (assoc opts :green/exit 0))

(defn resource-step
  "Apply the RedisDeployment. A suspended resource is never touched: an apply
  would not reset `spec.suspend`, but converging over an acknowledged
  suspension (a rehearsal in progress) is exactly what suspension forbids."
  [opts]
  (let [current (tools/get-resource opts)]
    (when (and current (tools/suspended? current))
      (throw (ex-info (str "RedisDeployment " (:namespace opts) "/" (:resource-name opts)
                           " is suspended; verify no workflow runs, resume it by hand, then create again")
                      {:green/exit 1})))
    (when (and current (tools/deleting? current))
      (throw (ex-info "RedisDeployment is being deleted; wait for the finalizer before creating again" {})))
    (tools/apply! opts (tools/resource opts))
    (tools/log "applied RedisDeployment" (str (:namespace opts) "/" (:resource-name opts)))
    (assoc opts :green/exit 0)))

(defn ready-step [opts]
  (let [cr (operator/wait-ready opts {:timeout-ms 2700000})]
    (tools/log "ready" (tools/phase-line cr) (str "provider-id=" (get-in cr [:status :providerId] "unpublished")))
    (assoc opts :green/exit 0)))

;; ---------------------------------------------------------------- delete

(defn destroy-step
  "Reaching this step means the destruction guard was lifted, which selects
  Destroy regardless of the committed `deletion-policy`. The controller
  runs the package delete under its finalizer; this step only waits."
  [opts]
  (let [current (tools/get-resource opts)]
    (cond
      (nil? current)
      (tools/log "RedisDeployment absent" (str (:namespace opts) "/" (:resource-name opts)))

      (tools/suspended? current)
      (throw (ex-info (str "RedisDeployment is suspended; verify no workflow runs and resume it by hand before delete")
                      {:green/exit 1}))

      :else
      (do
        (when-not (= "Destroy" (get-in current [:spec :deletionPolicy]))
          (tools/patch-resource! opts current [{:op "add" :path "/spec/deletionPolicy" :value "Destroy"}])
          (tools/log "patched deletionPolicy=Destroy"))
        (when-not (tools/deleting? current)
          (tools/kubectl opts ["delete" tools/resource-type (:resource-name opts) "-n" (:namespace opts)
                               "--wait=false" "--ignore-not-found"]))
        (let [note (operator/change-logger "finalizing")]
          (operator/wait-for
           {:label "RedisDeployment finalizer" :timeout-ms 1800000 :interval-ms 15000}
           (fn [] (let [cr (tools/get-resource opts)]
                    (if (nil? cr) :gone (do (note (tools/phase-line cr)) nil))))))
        (tools/log "RedisDeployment removed after finalization"))))
  (assoc opts :green/exit 0))

(defn namespace-delete-step [opts]
  (tools/kubectl opts ["delete" "namespace" (:namespace opts) "--ignore-not-found" "--timeout=900s"]
                 {:request-timeout? false :timeout-ms 930000})
  (tools/log "namespace removed" (:namespace opts))
  (assoc opts :green/exit 0))

(defn crd-delete-step
  "The CRD is cluster-scoped and may serve other namespaces: remove it only
  when no RedisDeployment remains anywhere."
  [opts]
  (let [remaining (:items (tools/kubectl opts ["get" tools/resource-type "-A" "-o" "json"]
                                         {:json? true :not-found {:items []}}))]
    (if (seq remaining)
      (tools/log "CRD retained:" (count remaining) "RedisDeployment(s) remain in other namespaces")
      (do (tools/kubectl opts ["delete" "crd" tools/crd-name "--ignore-not-found"])
          (tools/log "CRD removed" tools/crd-name))))
  (assoc opts :green/exit 0))

;; ---------------------------------------------------------------- graph

(defn wire-fn [step opts]
  (case (:green/event opts)
    :build (case step
             :redis-operator/start [start-step :redis-operator/render]
             :redis-operator/render [render-step])
    :create (case step
              :redis-operator/start [start-step :redis-operator/render]
              :redis-operator/render [render-step :redis-operator/namespace]
              :redis-operator/namespace [namespace-step :redis-operator/credentials]
              :redis-operator/credentials [credentials-step :redis-operator/pull-secret]
              :redis-operator/pull-secret [pull-secret-step :redis-operator/install]
              :redis-operator/install [install-step :redis-operator/resource]
              :redis-operator/resource [resource-step :redis-operator/ready]
              :redis-operator/ready [ready-step])
    :delete (case step
              :redis-operator/start [start-step :redis-operator/destroy]
              :redis-operator/destroy [destroy-step :redis-operator/namespace-delete]
              :redis-operator/namespace-delete [namespace-delete-step :redis-operator/crd-delete]
              :redis-operator/crd-delete [crd-delete-step])
    :check (case step
             :redis-operator/start [start-step :redis-operator/check]
             :redis-operator/check [operator/check-step])
    :rehearse (case step
                :redis-operator/start [start-step :redis-operator/rehearse]
                :redis-operator/rehearse [operator/rehearse-step])
    :drill (case step
             :redis-operator/start [start-step :redis-operator/drill]
             :redis-operator/drill [operator/drill-step])
    :restart (case step
               :redis-operator/start [start-step :redis-operator/restart]
               :redis-operator/restart [operator/restart-step])))

(def side-effecting
  [:redis-operator/namespace :redis-operator/credentials :redis-operator/pull-secret
   :redis-operator/install :redis-operator/resource :redis-operator/ready
   :redis-operator/destroy :redis-operator/namespace-delete :redis-operator/crd-delete
   :redis-operator/check :redis-operator/rehearse :redis-operator/drill :redis-operator/restart])

(defn- quiet-failure
  "Operational failures are reported by message, not stack trace: the text
  of a kubectl or API refusal is the diagnosis."
  [f opts]
  (try (f opts)
       (catch Exception e
         (assoc opts :green/exit (or (:green/exit (ex-data e)) 1)
                :green/err (or (ex-message e) (str (class e)))))))

(def workflow
  (-> (wf/workflow {:start :redis-operator/start :wire-fn wire-fn
                    :next-fn (fn [_ successors opts]
                               (if (wf/failed? opts) [] (mapv #(vector % opts) successors)))})
      (wf/advice-add-all :around ::quiet quiet-failure)
      progress/advise
      (dry-run/advise side-effecting)))
