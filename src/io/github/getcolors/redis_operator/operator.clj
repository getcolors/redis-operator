(ns io.github.getcolors.redis-operator.operator
  "The operational verbs: `check`, `rehearse`, `drill` and `restart`. Each
  is a port of the Python script the redis-doks deployment ran, with the
  same preconditions, the same fail-closed ownership checks and the same
  evidence shape, written under `.colors/<profile>/evidence/`."
  (:require [clojure.string :as str]
            [io.github.getcolors.redis-operator.tools :as tools]))

(def sleep! (fn [ms] (Thread/sleep (long ms))))

(defn change-logger
  "A function that logs `line` under `label` only when it differs from the
  last one it saw, so a long wait prints each state transition once."
  [label]
  (let [last (atom nil)]
    (fn [line]
      (when (and line (not= line @last))
        (reset! last line)
        (tools/log label line)))))

(defn wait-for
  "Call `f` every `interval-ms` until it returns a truthy value, which is
  returned. A fatal exception (tools/fatal) aborts at once; any other
  exception is retried until `timeout-ms`, after which the last error is
  reported with the label."
  [{:keys [label timeout-ms interval-ms] :or {interval-ms 15000}} f]
  (let [deadline (+ (System/nanoTime) (* (long timeout-ms) 1000000))]
    (loop [last-error nil]
      (let [outcome (try (or (f) ::pending)
                         (catch Exception e
                           (if (tools/fatal? e) (throw e) e)))]
        (cond
          (and (not= ::pending outcome) (not (instance? Throwable outcome))) outcome
          (<= (- deadline (System/nanoTime)) 0)
          (throw (ex-info (str "timed out waiting for " label
                               (when-let [e (if (instance? Throwable outcome) outcome last-error)]
                                 (str "; last error: " (ex-message e))))
                          {:timeout? true}))
          :else (do (sleep! (min interval-ms (max 0 (quot (- deadline (System/nanoTime)) 1000000))))
                    (recur (if (instance? Throwable outcome) outcome last-error))))))))

(defn require-active!
  "A resource under management: present, not suspended, not being deleted."
  [opts cr]
  (when (nil? cr)
    (throw (tools/fatal (str "RedisDeployment " (:namespace opts) "/" (:resource-name opts) " does not exist"))))
  (when (tools/suspended? cr)
    (throw (tools/fatal "RedisDeployment is suspended; verify no workflow runs before resuming it by hand")))
  (when (tools/deleting? cr)
    (throw (tools/fatal "RedisDeployment is being deleted")))
  cr)

(defn wait-ready
  "Wait through routine reconciliation for Ready at the current generation,
  printing each phase transition. Suspension, deletion and an Invalid or
  Blocked phase end the wait: none of them heals by waiting."
  [opts {:keys [timeout-ms] :or {timeout-ms 600000}}]
  (let [note (change-logger "waiting")]
    (wait-for
     {:label "Ready at the current generation" :timeout-ms timeout-ms}
     (fn []
       (let [cr (require-active! opts (tools/get-resource opts))
             phase (get-in cr [:status :phase])]
         (note (tools/phase-line cr))
         (when (contains? #{"Invalid" "Blocked"} phase)
           (throw (tools/fatal (str "RedisDeployment reports " (tools/phase-line cr)))))
         (when (tools/ready? cr) cr))))))

(defn healthy-probe!
  "The health probe, which must report a live authenticated PING."
  [opts]
  (let [result (tools/probe opts "health")]
    (when-not (true? (:healthy result))
      (throw (tools/fatal "Redis must be healthy before the operation")))
    result))

(defn summary [probe]
  (str "provider-id=" (:providerId probe) " name=" (:name probe) " ip=" (:ip probe)
       " profile=" (:profile probe)
       (when (contains? probe :healthy) (str " healthy=" (:healthy probe)))))

(defn- outcome [opts f]
  (try (f) (assoc opts :green/exit 0)
       (catch Exception e
         (assoc opts :green/exit (or (:green/exit (ex-data e)) 1)
                :green/err (or (ex-message e) (str (class e)))))))

;; ----------------------------------------------------------------- check

(defn check-step [opts]
  (outcome opts
           (fn []
             (let [cr (require-active! opts (tools/get-resource opts))]
               (when-not (tools/ready? cr)
                 (throw (tools/fatal (str "RedisDeployment is not Ready at its current generation: "
                                          (tools/phase-line cr)))))
               (let [probe (healthy-probe! opts)]
                 (tools/log "check" (tools/phase-line cr))
                 (tools/log "check" (summary probe)))))))

;; ----------------------------------------------------------------- rehearse

(defn set-suspended! [opts current value]
  (tools/patch-resource! opts current [{:op "add" :path "/spec/suspend" :value value}]))

(defn- unchanged? [cr evidence generation-key]
  (and (= (get-in cr [:metadata :uid]) (:resourceUID evidence))
       (= (get-in cr [:metadata :generation]) (get evidence generation-key))))

(def suspended-warning
  "resource left suspended; verify no workflow runs before resuming")

(defn rehearse!
  "Suspend, wait for the controller's acknowledgement, run the package's
  backup rehearsal through the probe, resume. Every uncertain path leaves
  the resource suspended and says so; a failed re-read never masks the
  error that preceded it."
  [opts]
  (let [write! (fn [evidence] (tools/write-evidence! opts "backup-rehearsal" evidence))
        original (wait-ready opts {})
        evidence (atom {:startedAt (tools/now) :resourceUID (get-in original [:metadata :uid])
                        :passed false})
        _ (write! @evidence)
        suspended (set-suspended! opts original true)
        _ (swap! evidence assoc :suspendedGeneration (get-in suspended [:metadata :generation]))
        _ (write! @evidence)
        _ (tools/log "rehearse" "suspend requested at generation" (:suspendedGeneration @evidence))
        note (change-logger "rehearse")
        failure (atom nil)
        uncertain (atom false)]
    ;; Phase one: the rehearsal itself. Errors are recorded, not thrown, so
    ;; the resume decision below sees them.
    (try
      (wait-for
       {:label "acknowledged suspension" :timeout-ms 7800000 :interval-ms 3000}
       (fn []
         (let [current (tools/get-resource opts)]
           (when-not (and current (unchanged? current @evidence :suspendedGeneration))
             (throw (tools/fatal "Resource changed while awaiting suspension")))
           (note (tools/phase-line current))
           (when (tools/acknowledged-suspension? current) current))))
      (swap! evidence assoc :suspensionAcknowledgedAt (tools/now))
      (write! @evidence)
      (let [result (try (tools/probe opts "rehearse")
                        (catch Exception e
                          ;; kubectl's termination cannot prove the remote workflow stopped.
                          (reset! uncertain true)
                          (throw e)))]
        (swap! evidence assoc :result result)
        (when-not (true? (:rehearsalPassed result))
          (throw (ex-info "Backup rehearsal failed" {})))
        (swap! evidence assoc :passed true))
      (catch Exception e (reset! failure e)))
    ;; Phase two: resume only when the resource is exactly as we left it and
    ;; the rehearsal's completion is certain.
    (let [current (try (tools/get-resource opts)
                       (catch Exception e (swap! evidence assoc :resumeBlocked (str "could not re-read the resource: " (ex-message e))) ::unreadable))]
      (cond
        (= ::unreadable current) nil
        @uncertain
        (swap! evidence assoc :resumeBlocked "Remote execution completion is uncertain; confirm it stopped before resuming")
        (not (and current (unchanged? current @evidence :suspendedGeneration)))
        (swap! evidence assoc :resumeBlocked "Resource changed during rehearsal; suspension was not overwritten")
        :else
        (try (let [resumed (set-suspended! opts current false)]
               (swap! evidence assoc :resumedGeneration (get-in resumed [:metadata :generation])
                      :resumedAt (tools/now)))
             (catch Exception e
               (swap! evidence assoc :resumeBlocked (str "resume patch failed: " (ex-message e)))))))
    (let [path (write! @evidence)
          blocked (:resumeBlocked @evidence)]
      (when blocked (tools/log "rehearse" suspended-warning))
      (cond
        @failure (throw (ex-info (str "Backup rehearsal failed: " (ex-message @failure)
                                      (when blocked (str "; " blocked "; " suspended-warning))
                                      "; evidence: " path) {}))
        blocked (throw (ex-info (str blocked "; " suspended-warning "; evidence: " path) {}))
        :else
        (do
          (tools/log "rehearse" "resumed at generation" (:resumedGeneration @evidence))
          (let [cr (wait-ready opts {:timeout-ms 2700000})]
            (tools/log "rehearse" "passed;" (tools/phase-line cr) "; evidence:" path)))))))

(defn rehearse-step [opts] (outcome opts #(rehearse! opts)))

;; ----------------------------------------------------------------- drill

(def drill-timeout-ms 2400000)

(defn worker-ids
  "Droplet IDs of every node pool of the DOKS cluster, when the deployment
  names it; without the cluster ID the drill excludes workers by their
  `k8s:` tags and by name only."
  [token cluster-id]
  (if cluster-id
    (set (for [pool (get-in (tools/digitalocean token :get (str "kubernetes/clusters/" cluster-id))
                            [:kubernetes_cluster :node_pools])
               node (:nodes pool)]
           (str (:droplet_id node))))
    #{}))

(defn- uuid-hex [] (str/replace (str (java.util.UUID/randomUUID)) "-" ""))

(defn drill!
  "Delete exactly the owned Redis Droplet and wait for the controller to
  replace it with a healthy one at an unchanged resource UID and generation."
  [opts]
  (let [token (:do-token opts)
        write! (fn [evidence] (tools/write-evidence! opts "self-healing" evidence))
        profile (:profile opts)
        cluster-id (:doks-cluster-id opts)
        cr (wait-ready opts {})
        before (healthy-probe! opts)
        workers (worker-ids token cluster-id)
        droplet (:droplet (tools/digitalocean token :get (str "droplets/" (:providerId before))))]
    (tools/owned-droplet! droplet before profile workers)
    (when (nil? cluster-id)
      (tools/log "drill" "doks-cluster-id absent: workers excluded by k8s: tag and name only"))
    (let [marker-key (str "colors:self-heal:" (uuid-hex)) marker-value (uuid-hex)]
      (when-not (= marker-value (:marker (tools/probe opts "set-marker" marker-key marker-value)))
        (throw (tools/fatal "Authenticated Redis SET/GET failed before disruption")))
      (let [evidence (atom {:startedAt (tools/now) :clusterId cluster-id
                            :resourceUID (get-in cr [:metadata :uid])
                            :generation (get-in cr [:metadata :generation])
                            :before before :excludedWorkerIds (vec (sort workers))
                            :markerKey marker-key :markerValue marker-value
                            :expectedDataRecovery false :passed false})]
        (write! @evidence)
        (tools/log "drill" "deleting owned Droplet" (:providerId before) (str "ip=" (:ip before)))
        (tools/digitalocean token :delete (str "droplets/" (:providerId before)))
        (swap! evidence assoc :deleteAcceptedAt (tools/now))
        (write! @evidence)
        (let [note (change-logger "drill")
              result
              (try
                (wait-for
                 {:label "service recovery" :timeout-ms drill-timeout-ms :interval-ms 15000}
                 (fn []
                   (let [current (tools/get-resource opts)]
                     (when-not (and current (unchanged? current @evidence :generation))
                       (throw (tools/fatal "Desired state changed during recovery test")))
                     (note (tools/phase-line current))
                     ;; A probe that cannot run yet, or prints nothing parseable
                     ;; mid-recreate, is a reason to look again, not to stop.
                     (let [after (tools/probe opts "health")]
                       (when (and (not= (:providerId after) (:providerId before))
                                  (true? (:healthy after)) (tools/ready? current))
                         (let [live (:droplet (tools/digitalocean token :get (str "droplets/" (:providerId after))))]
                           (tools/owned-droplet! live after profile workers)
                           (when (some? (tools/digitalocean token :get (str "droplets/" (:providerId before))))
                             (throw (tools/fatal "Original Droplet still exists")))
                           (let [survived (= marker-value (:marker (tools/probe opts "get-marker" marker-key)))
                                 recovered-value (uuid-hex)
                                 written (= recovered-value
                                            (:marker (tools/probe opts "set-marker" marker-key recovered-value)))]
                             (swap! evidence assoc :recoveredAt (tools/now) :after after
                                    :priorMarkerSurvived survived :authenticatedWriteReadPassed written)
                             (when-not written (throw (tools/fatal "Replacement write/read failed")))
                             after)))))))
                (catch Exception e
                  (swap! evidence assoc :passed false :timedOutAt (tools/now)
                         :lastErrorType (if (tools/fatal? e) "fatal" "timeout")
                         :lastError (ex-message e))
                  (write! @evidence)
                  (throw (ex-info (str (if (tools/fatal? e) "Self-healing drill failed: " "Recovery timed out: ")
                                       (ex-message e) "; inspect recorded evidence and controller status") {}))))]
          (swap! evidence assoc :passed true)
          (let [path (write! @evidence)]
            (tools/log "drill" "service recovery verified;" (summary result)
                       (str "prior-marker-survived=" (:priorMarkerSurvived @evidence)) "; evidence:" path)))))))

(defn drill-step [opts] (outcome opts #(drill! opts)))

;; ----------------------------------------------------------------- restart

(defn restart!
  "Scale the controller to zero, wait for the old pod to finish, scale back
  to one, and wait for a reconcile that happened after the restart before
  judging: the same Droplet, healthy, no new convergence, same UID and
  generation."
  [opts]
  (let [write! (fn [evidence] (tools/write-evidence! opts "controller-restart" evidence))
        namespace (:namespace opts)
        deployment (str "deployment/" tools/controller-name)
        cr (wait-ready opts {})
        before (healthy-probe! opts)
        replicas (get-in (tools/kubectl opts ["get" "deployment" tools/controller-name "-n" namespace "-o" "json"]
                                        {:json? true})
                         [:spec :replicas])]
    (when-not (= 1 replicas)
      (throw (tools/fatal "Restart test requires exactly one controller replica")))
    (let [baseline (get-in cr [:status :lastReconcileTime])
          evidence (atom {:startedAt (tools/now) :before before
                          :resourceUID (get-in cr [:metadata :uid])
                          :generation (get-in cr [:metadata :generation])
                          :lastReconcileTimeBefore baseline :passed false})]
      (write! @evidence)
      (tools/kubectl opts ["scale" deployment "-n" namespace "--replicas=0"])
      (tools/log "restart" "scaled to zero; waiting for the old controller to stop")
      ;; Never force deletion or start another controller while the old
      ;; process may still be running a workflow.
      (try
        (wait-for
         {:label "old controller pod to stop" :timeout-ms 7800000 :interval-ms 5000}
         (fn []
           (let [pods (:items (tools/kubectl opts ["get" "pods" "-n" namespace "-l" (str "app=" tools/controller-name) "-o" "json"]
                                             {:json? true}))]
             (when (empty? pods) :stopped))))
        (catch Exception e
          (swap! evidence assoc :stoppedAt nil :failure "Old controller did not stop; intentionally leaving replicas at zero")
          (write! @evidence)
          (throw (ex-info (str "Old controller did not stop; intentionally leaving replicas at zero: " (ex-message e)) {}))))
      (swap! evidence assoc :stoppedAt (tools/now))
      (write! @evidence)
      (tools/kubectl opts ["scale" deployment "-n" namespace "--replicas=1"])
      (tools/kubectl opts ["rollout" "status" deployment "-n" namespace "--timeout=600s"]
                     {:request-timeout? false :timeout-ms 630000})
      (swap! evidence assoc :rolledOutAt (tools/now))
      (write! @evidence)
      (let [note (change-logger "restart")
            current (wait-for
                     {:label "a reconcile after the restart" :timeout-ms 1800000 :interval-ms 10000}
                     (fn []
                       (let [current (require-active! opts (tools/get-resource opts))
                             reconciled (get-in current [:status :lastReconcileTime])]
                         (note (str (tools/phase-line current) " lastReconcileTime=" reconciled))
                         (when (and (tools/ready? current)
                                    (or (nil? baseline) (tools/instant-after? reconciled baseline)))
                           current))))
            after (tools/probe opts "health")]
        (when-not (and (true? (:healthy after)) (= (:providerId before) (:providerId after)))
          (throw (tools/fatal "Restart did not preserve healthy infrastructure")))
        (when-not (and (:convergenceRecordModifiedMs before)
                       (= (:convergenceRecordModifiedMs before) (:convergenceRecordModifiedMs after)))
          (throw (tools/fatal "Restart unexpectedly ran another convergence workflow")))
        (when-not (unchanged? current @evidence :generation)
          (throw (tools/fatal "Desired state changed during restart test")))
        (swap! evidence assoc :passed true :after after
               :lastReconcileTimeAfter (get-in current [:status :lastReconcileTime])
               :completedAt (tools/now))
        (let [path (write! @evidence)]
          (tools/log "restart" "controller restart preserved Redis;" (summary after) "; evidence:" path))))))

(defn restart-step [opts] (outcome opts #(restart! opts)))
