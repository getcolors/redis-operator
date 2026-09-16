(ns io.github.getcolors.redis-operator.operator-test
  (:require [babashka.fs :as fs]
            [cheshire.core :as json]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [io.github.getcolors.redis-operator.operator :as operator]
            [io.github.getcolors.redis-operator.tools :as tools]))

(defmacro with-tmp [[sym] & body]
  `(let [~sym (fs/create-temp-dir)]
     (try ~@body (finally (fs/delete-tree ~sym)))))

(defn opts [tmp]
  {:profile "redis-dev" :resource-name "redis-dev" :namespace "colors-redis" :kube-context "ctx"
   :workdir (str tmp) :green/state-file (str (fs/path tmp "colors.yml")) :do-token "token"})

(defn ready-cr [generation & [extra]]
  (merge {:metadata {:uid "uid-1" :generation generation :resourceVersion (str generation)}
          :spec {:suspend false}
          :status {:phase "Ready" :observedGeneration generation :lastReconcileTime "2026-09-16T10:00:00Z"
                   :conditions [{:type "Ready" :status "True" :reason "Converged"}]}}
         extra))

(defn suspended-cr [generation]
  (-> (ready-cr generation) (assoc-in [:spec :suspend] true)
      (assoc-in [:status :phase] "Suspended")))

(defn evidence [tmp name]
  (json/parse-string (slurp (str (fs/path tmp "redis-dev" "evidence" (str name ".json")))) true))

(defn quiet [f] (with-out-str (f)))
(defn quiet-result [f] (let [r (atom nil)] (with-out-str (reset! r (f))) @r))

(deftest wait-for-retries-until-deadline-and-aborts-on-fatal
  (with-redefs [operator/sleep! (fn [_] nil)]
    (let [calls (atom 0)]
      (is (= :done (operator/wait-for {:label "x" :timeout-ms 10000}
                                      (fn [] (if (< (swap! calls inc) 3) (throw (Exception. "transient")) :done)))))
      (is (= 3 @calls)))
    (let [e (try (operator/wait-for {:label "never" :timeout-ms 1} (fn [] (throw (Exception. "still failing")))) nil
                 (catch Exception e e))]
      (is (str/includes? (ex-message e) "timed out waiting for never"))
      (is (str/includes? (ex-message e) "still failing")))
    (let [calls (atom 0)]
      (is (thrown-with-msg? Exception #"stop now"
                            (operator/wait-for {:label "x" :timeout-ms 10000}
                                               (fn [] (swap! calls inc) (throw (tools/fatal "stop now"))))))
      (is (= 1 @calls)))))

(deftest wait-ready-never-overrides-suspension-or-deletion
  (with-tmp [tmp]
    (with-redefs [operator/sleep! (fn [_] nil)]
      (let [reads (atom [(assoc-in (ready-cr 2) [:status :phase] "Reconciling") (ready-cr 2)])]
        (with-redefs [tools/get-resource (fn [_] (let [r (first @reads)] (swap! reads rest) r))]
          (is (= (ready-cr 2) (quiet-result #(operator/wait-ready (opts tmp) {:timeout-ms 10000}))))))
      (doseq [[cr fragment] [[(suspended-cr 2) "suspended"]
                             [(assoc-in (ready-cr 2) [:metadata :deletionTimestamp] "now") "being deleted"]
                             [(assoc-in (ready-cr 2) [:status :phase] "Invalid") "Invalid"]
                             [nil "does not exist"]]]
        (with-redefs [tools/get-resource (constantly cr)]
          (let [e (try (quiet #(operator/wait-ready (opts tmp) {:timeout-ms 10000})) nil (catch Exception e e))]
            (is (some? e) fragment)
            (is (str/includes? (ex-message e) fragment)))))
      (testing "a stale Ready keeps waiting"
        (with-redefs [tools/get-resource (constantly (assoc-in (ready-cr 3) [:status :observedGeneration] 2))]
          (is (thrown-with-msg? Exception #"timed out" (quiet #(operator/wait-ready (opts tmp) {:timeout-ms 1})))))))))

(deftest check-requires-current-ready-and-healthy
  (with-tmp [tmp]
    (with-redefs [tools/get-resource (constantly (ready-cr 2))
                  tools/probe (fn [_ op & _] {:providerId "1" :name "redis-dev" :ip "1.2.3.4" :profile "redis-dev" :healthy true})]
      (is (zero? (:green/exit (quiet-result #(operator/check-step (opts tmp)))))))
    (with-redefs [tools/get-resource (constantly (ready-cr 2))
                  tools/probe (fn [_ op & _] {:providerId "1" :healthy false})]
      (let [r (quiet-result #(operator/check-step (opts tmp)))]
        (is (= 1 (:green/exit r)))
        (is (str/includes? (:green/err r) "healthy"))))
    (with-redefs [operator/sleep! (fn [_] nil)
                  tools/get-resource (constantly (assoc-in (ready-cr 2) [:status :observedGeneration] 1))
                  tools/probe (fn [& _] (throw (ex-info "must not probe" {})))
                  operator/wait-for (let [original operator/wait-for]
                                      (fn [config f] (original (assoc config :timeout-ms 1) f)))]
      (let [r (quiet-result #(operator/check-step (opts tmp)))]
        (is (= 1 (:green/exit r)))
        (is (str/includes? (:green/err r) "timed out waiting for Ready at the current generation"))))
    (with-redefs [tools/get-resource (constantly (suspended-cr 2))]
      (is (str/includes? (:green/err (quiet-result #(operator/check-step (opts tmp)))) "suspended")))))

(deftest rehearse-suspends-runs-and-resumes
  (with-tmp [tmp]
    (with-redefs [operator/sleep! (fn [_] nil)]
      (testing "the happy path"
        (let [state (atom (ready-cr 2)) patches (atom [])]
          (with-redefs [tools/get-resource (fn [_] @state)
                        tools/patch-resource! (fn [_ current patch]
                                                (swap! patches conj [(get-in current [:metadata :resourceVersion]) patch])
                                                (let [value (:value (first patch))]
                                                  (reset! state (if value (suspended-cr 3) (ready-cr 4)))))
                        tools/probe (fn [_ op & _] (is (= "rehearse" op)) {:rehearsalPassed true})]
            (let [r (quiet-result #(operator/rehearse-step (opts tmp)))]
              (is (zero? (:green/exit r)) (:green/err r))
              (is (= [["2" [{:op "add" :path "/spec/suspend" :value true}]]
                      ["3" [{:op "add" :path "/spec/suspend" :value false}]]] @patches))
              (let [e (evidence tmp "backup-rehearsal")]
                (is (true? (:passed e)))
                (is (= 3 (:suspendedGeneration e)))
                (is (= 4 (:resumedGeneration e)))
                (is (true? (get-in e [:result :rehearsalPassed]))))))))
      (testing "an uncertain probe leaves the resource suspended and says so"
        (let [state (atom (ready-cr 2)) patches (atom [])]
          (with-redefs [tools/get-resource (fn [_] @state)
                        tools/patch-resource! (fn [_ _ patch] (swap! patches conj patch) (reset! state (suspended-cr 3)))
                        tools/probe (fn [& _] (throw (ex-info "kubectl exec failed (exit 124)" {})))]
            (let [out (atom nil) r (quiet-result #(let [r (operator/rehearse-step (opts tmp))] (reset! out r) r))]
              (is (= 1 (:green/exit r)))
              (is (str/includes? (:green/err r) operator/suspended-warning))
              (is (str/includes? (:green/err r) "kubectl exec failed"))
              (is (= 1 (count @patches)))
              (let [e (evidence tmp "backup-rehearsal")]
                (is (false? (:passed e)))
                (is (str/includes? (:resumeBlocked e) "uncertain")))))))
      (testing "a failed rehearsal still resumes, and a failed re-read does not mask it"
        (let [state (atom (ready-cr 2)) patches (atom [])]
          (with-redefs [tools/get-resource (fn [_] @state)
                        tools/patch-resource! (fn [_ _ patch] (swap! patches conj patch)
                                                (reset! state (if (:value (first patch)) (suspended-cr 3) (ready-cr 4))))
                        tools/probe (fn [& _] {:rehearsalPassed false})]
            (let [r (quiet-result #(operator/rehearse-step (opts tmp)))]
              (is (= 1 (:green/exit r)))
              (is (str/includes? (:green/err r) "Backup rehearsal failed"))
              (is (= 2 (count @patches)))
              (is (= 4 (:resumedGeneration (evidence tmp "backup-rehearsal")))))))
        (let [reads (atom 0)]
          (with-redefs [tools/get-resource (fn [_] (case (swap! reads inc) 1 (ready-cr 2) 2 (suspended-cr 3) (throw (ex-info "connection refused" {}))))
                        tools/patch-resource! (fn [_ _ _] (suspended-cr 3))
                        tools/probe (fn [& _] {:rehearsalPassed false})]
            (let [r (quiet-result #(operator/rehearse-step (opts tmp)))]
              (is (= 1 (:green/exit r)))
              (is (str/includes? (:green/err r) "Backup rehearsal failed"))
              (is (str/includes? (:green/err r) "could not re-read"))
              (is (str/includes? (:green/err r) operator/suspended-warning))))))
      (testing "a resource changed by someone else is left alone"
        (let [state (atom (ready-cr 2))]
          (with-redefs [tools/get-resource (fn [_] @state)
                        tools/patch-resource! (fn [_ _ _] (reset! state (suspended-cr 3)) (suspended-cr 3))
                        tools/probe (fn [& _] (reset! state (suspended-cr 5)) {:rehearsalPassed true})]
            (let [r (quiet-result #(operator/rehearse-step (opts tmp)))]
              (is (= 1 (:green/exit r)))
              (is (str/includes? (:green/err r) "Resource changed")))))))))

(def probe-before {:providerId "100" :name "redis-dev" :ip "1.1.1.1" :profile "redis-dev" :healthy true
                   :convergenceRecordModifiedMs 5})
(def probe-after {:providerId "200" :name "redis-dev" :ip "2.2.2.2" :profile "redis-dev" :healthy true
                  :convergenceRecordModifiedMs 9})
(defn droplet [id ip & [tags]]
  {:droplet {:id id :name "redis-dev" :tags (or tags []) :networks {:v4 [{:type "public" :ip_address ip}]}}})

(deftest drill-deletes-exactly-the-owned-droplet-and-waits-for-recovery
  (with-tmp [tmp]
    (with-redefs [operator/sleep! (fn [_] nil)]
      (let [deleted (atom []) markers (atom {}) probes (atom 0)
            api (fn [_ method path]
                  (case [method path]
                    [:get "kubernetes/clusters/a87775cd-de9f-4390-8dee-281f864bc9de"]
                    {:kubernetes_cluster {:node_pools [{:nodes [{:droplet_id 555}]}]}}
                    [:get "droplets/100"] (when (empty? @deleted) (droplet 100 "1.1.1.1"))
                    [:get "droplets/200"] (droplet 200 "2.2.2.2")
                    [:delete "droplets/100"] (do (swap! deleted conj path) nil)))]
        (with-redefs [tools/get-resource (constantly (ready-cr 3))
                      tools/digitalocean api
                      tools/probe (fn [_ op & [k v]]
                                    (case op
                                      "health" (if (< (swap! probes inc) 3) probe-before probe-after)
                                      "set-marker" (do (swap! markers assoc k v) {:marker v})
                                      "get-marker" {:marker (get @markers k)}))]
          (let [r (quiet-result #(operator/drill-step (assoc (opts tmp) :doks-cluster-id "a87775cd-de9f-4390-8dee-281f864bc9de")))]
            (is (zero? (:green/exit r)) (:green/err r))
            (is (= ["droplets/100"] @deleted))
            (let [e (evidence tmp "self-healing")]
              (is (true? (:passed e)))
              (is (= ["555"] (:excludedWorkerIds e)))
              (is (= "100" (get-in e [:before :providerId])))
              (is (= "200" (get-in e [:after :providerId])))
              (is (true? (:priorMarkerSurvived e)))
              (is (true? (:authenticatedWriteReadPassed e)))
              (is (false? (:expectedDataRecovery e)))))))
      (testing "an unowned Droplet is never deleted"
        (doseq [live [(droplet 100 "9.9.9.9") (droplet 100 "1.1.1.1" ["k8s:worker"]) (assoc-in (droplet 100 "1.1.1.1") [:droplet :name] "other")]]
          (let [deleted (atom [])]
            (with-redefs [tools/get-resource (constantly (ready-cr 3))
                          tools/digitalocean (fn [_ method path]
                                               (when (= :delete method) (swap! deleted conj path))
                                               (when (= [:get "droplets/100"] [method path]) live))
                          tools/probe (fn [_ op & _] (case op "health" probe-before {:marker "x"}))]
              (let [r (quiet-result #(operator/drill-step (opts tmp)))]
                (is (= 1 (:green/exit r)))
                (is (empty? @deleted)))))))
      (testing "a worker id from the cluster is refused even when tags are missing"
        (let [deleted (atom [])]
          (with-redefs [tools/get-resource (constantly (ready-cr 3))
                        tools/digitalocean (fn [_ method path]
                                             (case [method path]
                                               [:get "kubernetes/clusters/a87775cd-de9f-4390-8dee-281f864bc9de"]
                                               {:kubernetes_cluster {:node_pools [{:nodes [{:droplet_id 100}]}]}}
                                               [:get "droplets/100"] (droplet 100 "1.1.1.1")
                                               [:delete "droplets/100"] (swap! deleted conj path)))
                        tools/probe (fn [_ op & _] (case op "health" probe-before {:marker "x"}))]
            (let [r (quiet-result #(operator/drill-step (assoc (opts tmp) :doks-cluster-id "a87775cd-de9f-4390-8dee-281f864bc9de")))]
              (is (str/includes? (:green/err r) "Kubernetes worker"))
              (is (empty? @deleted))))))
      (testing "unhealthy Redis stops the drill before any deletion"
        (let [deleted (atom [])]
          (with-redefs [tools/get-resource (constantly (ready-cr 3))
                        tools/digitalocean (fn [_ method path] (when (= :delete method) (swap! deleted conj path)))
                        tools/probe (fn [& _] (assoc probe-before :healthy false))]
            (let [r (quiet-result #(operator/drill-step (opts tmp)))]
              (is (= 1 (:green/exit r)))
              (is (empty? @deleted))))))
      (testing "unparseable probe output during recovery is retried, a changed generation is fatal"
        (let [reads (atom 0)]
          (with-redefs [tools/get-resource (fn [_] (if (< (swap! reads inc) 4) (ready-cr 3) (ready-cr 4)))
                        tools/digitalocean (fn [_ method path] (when (= [:get "droplets/100"] [method path]) (droplet 100 "1.1.1.1")))
                        tools/probe (fn [_ op & [k v]] (case op
                                                         "health" (if (< @reads 3) probe-before (throw (ex-info "JSON error" {})))
                                                         "set-marker" {:marker v}))]
            (let [r (quiet-result #(operator/drill-step (opts tmp)))]
              (is (= 1 (:green/exit r)))
              (is (str/includes? (:green/err r) "Desired state changed"))
              (is (false? (:passed (evidence tmp "self-healing")))))))))))

(deftest restart-waits-for-a-reconcile-after-the-restart
  (with-tmp [tmp]
    (with-redefs [operator/sleep! (fn [_] nil)]
      (let [calls (atom []) reads (atom 0) pods (atom 2)
            later (assoc-in (ready-cr 3) [:status :lastReconcileTime] "2026-09-16T10:05:00Z")]
        (with-redefs [tools/get-resource (fn [_] (if (< (swap! reads inc) 4) (ready-cr 3) later))
                      tools/kubectl (fn [_ args & _]
                                      (swap! calls conj args)
                                      (case (first args)
                                        "get" (if (= "deployment" (second args))
                                                {:spec {:replicas 1}}
                                                {:items (if (pos? (swap! pods dec)) [{}] [])})
                                        nil))
                      tools/probe (fn [& _] probe-before)]
          (let [r (quiet-result #(operator/restart-step (opts tmp)))]
            (is (zero? (:green/exit r)) (:green/err r))
            (is (some #(= ["scale" "deployment/colors-redis-operator" "-n" "colors-redis" "--replicas=0"] %) @calls))
            (is (some #(= ["scale" "deployment/colors-redis-operator" "-n" "colors-redis" "--replicas=1"] %) @calls))
            (let [e (evidence tmp "controller-restart")]
              (is (true? (:passed e)))
              (is (= "2026-09-16T10:00:00Z" (:lastReconcileTimeBefore e)))
              (is (= "2026-09-16T10:05:00Z" (:lastReconcileTimeAfter e)))))))
      (testing "a stale reconcile time never proves the restart"
        (with-redefs [tools/get-resource (constantly (ready-cr 3))
                      tools/kubectl (fn [_ args & _] (case (first args)
                                                       "get" (if (= "deployment" (second args)) {:spec {:replicas 1}} {:items []})
                                                       nil))
                      tools/probe (fn [& _] probe-before)
                      operator/wait-for (let [original operator/wait-for]
                                          (fn [config f] (original (if (= "a reconcile after the restart" (:label config))
                                                                     (assoc config :timeout-ms 1) config) f)))]
          (let [r (quiet-result #(operator/restart-step (opts tmp)))]
            (is (= 1 (:green/exit r)))
            (is (str/includes? (:green/err r) "a reconcile after the restart")))))
      (testing "a new convergence or a different Droplet fails the test"
        (let [reads (atom 0) later (assoc-in (ready-cr 3) [:status :lastReconcileTime] "2026-09-16T10:05:00Z")]
          (with-redefs [tools/get-resource (fn [_] (if (< (swap! reads inc) 2) (ready-cr 3) later))
                        tools/kubectl (fn [_ args & _] (case (first args)
                                                         "get" (if (= "deployment" (second args)) {:spec {:replicas 1}} {:items []})
                                                         nil))
                        tools/probe (let [n (atom 0)] (fn [& _] (if (= 1 (swap! n inc)) probe-before (assoc probe-before :convergenceRecordModifiedMs 6))))]
            (let [r (quiet-result #(operator/restart-step (opts tmp)))]
              (is (= 1 (:green/exit r)))
              (is (str/includes? (:green/err r) "another convergence"))))))
      (testing "more than one replica is refused"
        (with-redefs [tools/get-resource (constantly (ready-cr 3))
                      tools/kubectl (fn [_ args & _] (when (= "scale" (first args)) (throw (ex-info "must not scale" {}))) {:spec {:replicas 2}})
                      tools/probe (fn [& _] probe-before)]
          (is (str/includes? (:green/err (quiet-result #(operator/restart-step (opts tmp)))) "exactly one")))))))

(deftest ready-preconditions-poll-through-transient-reconciling
  (with-tmp [tmp]
    (with-redefs [operator/sleep! (fn [_] nil)
                  tools/failures (fn [_] {:count 0 :newest nil})]
      (testing "check: the first N reads are Reconciling at the current generation, then Ready"
        (let [reads (atom 0) reconciling (-> (ready-cr 1) (assoc-in [:status :phase] "Reconciling")
                                             (assoc-in [:status :conditions 0] {:type "Ready" :status "False" :reason "Reconciling"}))]
          (with-redefs [tools/get-resource (fn [_] (if (<= (swap! reads inc) 4) reconciling (ready-cr 1)))
                        tools/probe (fn [& _] probe-before)]
            (let [out (atom nil) r (quiet-result #(let [r (operator/check-step (opts tmp))] r))]
              (is (zero? (:green/exit r)) (:green/err r))
              (is (= 5 @reads))))))
      (testing "check: an unobserved generation is transient too"
        (let [reads (atom 0)]
          (with-redefs [tools/get-resource (fn [_] (if (< (swap! reads inc) 3) (assoc-in (ready-cr 2) [:status :observedGeneration] 1) (ready-cr 2)))
                        tools/probe (fn [& _] probe-before)]
            (is (zero? (:green/exit (quiet-result #(operator/check-step (opts tmp)))))))))
      (testing "check: Failed is an error, not a wait"
        (let [reads (atom 0)]
          (with-redefs [tools/get-resource (fn [_] (swap! reads inc) (assoc-in (ready-cr 1) [:status :phase] "Failed"))
                        tools/probe (fn [& _] (throw (ex-info "must not probe" {})))]
            (let [r (quiet-result #(operator/check-step (opts tmp)))]
              (is (= 1 (:green/exit r)))
              (is (str/includes? (:green/err r) "phase=Failed"))
              (is (= 1 @reads))))))
      (testing "check: Reconciling forever is the deadline, reported as such"
        (with-redefs [tools/get-resource (constantly (assoc-in (ready-cr 1) [:status :phase] "Reconciling"))
                      operator/wait-for (let [original operator/wait-for]
                                          (fn [config f] (original (assoc config :timeout-ms 1) f)))]
          (let [r (quiet-result #(operator/check-step (opts tmp)))]
            (is (= 1 (:green/exit r)))
            (is (str/includes? (:green/err r) "timed out waiting for Ready")))))
      (testing "the create wait tolerates a Failed pass, since the controller retries"
        (let [reads (atom 0)]
          (with-redefs [tools/get-resource (fn [_] (if (< (swap! reads inc) 3) (assoc-in (ready-cr 1) [:status :phase] "Failed") (ready-cr 1)))]
            (is (= (ready-cr 1) (quiet-result #(operator/wait-ready (opts tmp) {:timeout-ms 10000 :transient-failure? true}))))))))))

(deftest check-reports-retained-failures
  (with-tmp [tmp]
    (with-redefs [tools/get-resource (constantly (ready-cr 1))
                  tools/probe (fn [& _] probe-before)]
      (with-redefs [tools/failures (fn [_] {:count 2 :newest "20260916T101500Z-redis-ansible.log"})]
        (let [out (with-out-str (operator/check-step (opts tmp)))]
          (is (str/includes? out "failures retained: 2"))
          (is (str/includes? out "newest=20260916T101500Z-redis-ansible.log"))))
      (with-redefs [tools/failures (fn [_] {:count 0 :newest nil})]
        (is (str/includes? (with-out-str (operator/check-step (opts tmp))) "failures retained: 0")))
      (with-redefs [tools/failures (fn [_] {:error "kubectl exec failed"})]
        (let [out (atom nil) r (quiet-result #(let [r (operator/check-step (opts tmp))] (reset! out r) r))]
          (is (zero? (:green/exit r))))))))
