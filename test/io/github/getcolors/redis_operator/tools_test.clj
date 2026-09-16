(ns io.github.getcolors.redis-operator.tools-test
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [io.github.getcolors.redis-operator.tools :as tools]))

(def digest (apply str (repeat 64 "a")))
(def opts {:profile "redis-dev" :resource-name "redis-dev" :namespace "colors-redis"
           :kube-context "ctx" :image (str "registry.example/redis-operator@sha256:" digest)
           :deletion-policy "Retain" :reconcile-interval "60s"
           :provider-compute "digitalocean" :provider-backend "r2" :redis-port 6379})

(defn kind [manifests k] (first (filter #(= k (:kind %)) manifests)))
(defn pod-spec [manifests] (get-in (kind manifests "Deployment") [:spec :template :spec]))

(deftest manifests-render-the-installation
  (let [m (tools/manifests opts)]
    (is (= ["Namespace" "CustomResourceDefinition" "ServiceAccount" "Role" "RoleBinding"
            "PersistentVolumeClaim" "Deployment"] (mapv :kind m)))
    (is (= tools/crd-name (get-in (kind m "CustomResourceDefinition") [:metadata :name])))
    (testing "grace period exceeds the package's Ansible and OpenTofu caps"
      (is (= 10800 (:terminationGracePeriodSeconds (pod-spec m))))
      (is (> 10800 (+ 7200 1800))))
    (testing "the controller runs as root for /root/.ssh but without escalation or capabilities"
      (let [container (first (:containers (pod-spec m)))]
        (is (= {:allowPrivilegeEscalation false :capabilities {:drop ["ALL"]}
                :seccompProfile {:type "RuntimeDefault"}} (:securityContext container)))
        (is (nil? (:runAsNonRoot (:securityContext (pod-spec m)))))
        (is (= (:image opts) (:image container)))
        (is (= [{:secretRef {:name "redis-credentials"}}] (:envFrom container)))
        (is (some #(= "/root/.ssh" (:mountPath %)) (:volumeMounts container)))
        (testing "dependency caches live on the PVC so a restart resolves nothing"
          (is (= {"/data" nil "/root/.ssh" "ssh" "/root/.gitlibs" "gitlibs" "/root/.m2" "m2"
                  "/root/.deps.clj" "deps-clj" "/app/.cpcache" "cpcache"}
                 (into {} (map (juxt :mountPath :subPath) (:volumeMounts container)))))
          (is (every? #(= "state" (:name %)) (:volumeMounts container))))))
    (testing "pull secret only when named"
      (is (not (contains? (pod-spec m) :imagePullSecrets)))
      (is (= [{:name "colors-pull"}]
             (:imagePullSecrets (pod-spec (tools/manifests (assoc opts :image-pull-secret "colors-pull")))))))
    (testing "image and namespace shape are enforced at render time too"
      (is (thrown? Exception (tools/manifests (assoc opts :image "registry.example/redis-operator:latest"))))
      (is (thrown? Exception (tools/manifests (assoc opts :image (str "x@sha256:" (subs digest 1))))))
      (is (thrown? Exception (tools/manifests (assoc opts :namespace "Bad_Namespace")))))))

(deftest resource-renders-from-desired-state
  (let [cr (tools/resource (assoc opts :digitalocean-ssh-sources ["93.184.216.34/32"]))]
    (is (= "RedisDeployment" (:kind cr)))
    (is (= {:name "redis-dev" :namespace "colors-redis"} (:metadata cr)))
    (is (= "running" (get-in cr [:spec :state])))
    (is (= "Retain" (get-in cr [:spec :deletionPolicy])))
    (is (= "60s" (get-in cr [:spec :reconcileInterval])))
    (testing "suspend is never rendered, so an apply cannot reset it"
      (is (not (contains? (:spec cr) :suspend))))
    (is (= "redis-dev" (get-in cr [:spec :config :profile])))
    (is (= 6379 (get-in cr [:spec :config :redis-port])))
    (is (not (contains? (get-in cr [:spec :config]) :image)))
    (is (not (contains? (get-in cr [:spec :config]) :kube-context)))))

(deftest pretty-json-is-deterministic
  (let [a (tools/pretty-json {:b 1 :a {:d 2 :c 3}}) b (tools/pretty-json {:a {:c 3 :d 2} :b 1})]
    (is (= a b))
    (is (str/ends-with? a "\n"))
    (is (= {:a {:c 3 :d 2} :b 1} (json/parse-string a true)))))

(deftest kubectl-passes-context-and-never-kubeconfig
  (let [argv (tools/kubectl-args {:kube-context "do-ams3-x"} ["get" "pods"] {})]
    (is (= ["kubectl" "--context" "do-ams3-x" "--request-timeout=30s" "get" "pods"] argv))
    (is (not-any? #(str/includes? % "kubeconfig") argv))
    (is (= ["kubectl" "--context" "do-ams3-x" "exec" "x"]
           (tools/kubectl-args {:kube-context "do-ams3-x"} ["exec" "x"] {:request-timeout? false})))))

(deftest kubectl-errors-are-diagnosable-unless-quiet
  (with-redefs [tools/run-command (fn [_ _] {:exit 1 :out "" :err "error: context \"nope\" does not exist"})]
    (let [e (try (tools/kubectl opts ["get" "pods"]) nil (catch Exception e e))]
      (is (str/includes? (ex-message e) "does not exist")))
    (let [e (try (tools/kubectl opts ["apply" "-f" "-"] {:input "secret-doc" :quiet? true}) nil (catch Exception e e))]
      (is (str/includes? (ex-message e) "output suppressed"))
      (is (not (str/includes? (ex-message e) "does not exist")))))
  (testing "not-found is distinguished from every other failure"
    (with-redefs [tools/run-command (fn [_ _] {:exit 1 :out "" :err "Error from server (NotFound): secrets \"x\" not found"})]
      (is (= :absent (tools/kubectl opts ["get" "secret" "x"] {:not-found :absent}))))
    (with-redefs [tools/run-command (fn [_ _] {:exit 1 :out "" :err "error: You must be logged in to the server (Unauthorized)"})]
      (is (thrown? Exception (tools/kubectl opts ["get" "secret" "x"] {:not-found :absent}))))))

(deftest apply-sends-the-document-on-stdin
  (let [seen (atom nil)]
    (with-redefs [tools/run-command (fn [args {:keys [input]}] (reset! seen {:args args :input input}) {:exit 0 :out "" :err ""})]
      (tools/apply! opts {:kind "Secret" :stringData {:k "v"}} {:quiet? true})
      (is (= ["kubectl" "--context" "ctx" "--request-timeout=30s" "apply" "-f" "-"] (:args @seen)))
      (is (= {:kind "Secret" :stringData {:k "v"}} (json/parse-string (:input @seen) true)))
      (is (not-any? #(str/includes? (str %) "v") (:args @seen))))))

(deftest patch-tests-the-resource-version
  (let [seen (atom nil)]
    (with-redefs [tools/run-command (fn [args _] (reset! seen args) {:exit 0 :out "{}" :err ""})]
      (tools/patch-resource! opts {:metadata {:resourceVersion "42"}} [{:op "add" :path "/spec/suspend" :value true}])
      (is (= [{:op "test" :path "/metadata/resourceVersion" :value "42"}
              {:op "add" :path "/spec/suspend" :value true}]
             (json/parse-string (last @seen) true))))))

(defn with-out-str-value [f] (let [r (atom nil)] (with-out-str (reset! r (f))) @r))
(def stale-422 {:exit 1 :out "" :err "The request is invalid: the server rejected our request due to an error in our request"})
(defn patch-value [args] (:value (first (json/parse-string (last args) true))))
(def snapshot {:metadata {:uid "u" :resourceVersion "42"} :spec {:suspend false :deletionPolicy "Retain"}
               :status {:phase "Reconciling"}})

(deftest patch-retries-a-moved-resource-version-only
  (with-redefs [tools/sleep! (fn [_] nil)]
    (testing "a status write moved the resourceVersion: the second attempt carries the fresh one"
      (let [patches (atom [])]
        (with-redefs [tools/get-resource (constantly (-> snapshot (assoc-in [:metadata :resourceVersion] "43")
                                                         (assoc-in [:status :phase] "Ready")))
                      tools/run-command (fn [args _]
                                          (swap! patches conj (patch-value args))
                                          (if (= "42" (patch-value args)) stale-422 {:exit 0 :out "{\"ok\":true}" :err ""}))]
          (is (= {:ok true} (with-out-str-value #(tools/patch-resource! opts snapshot [{:op "add" :path "/spec/suspend" :value true}]))))
          (is (= ["42" "43"] @patches)))))
    (testing "spec changed concurrently: fail without retrying"
      (let [patches (atom 0)]
        (with-redefs [tools/get-resource (constantly (-> snapshot (assoc-in [:metadata :resourceVersion] "43")
                                                         (assoc-in [:spec :deletionPolicy] "Destroy")))
                      tools/run-command (fn [_ _] (swap! patches inc) stale-422)]
          (let [e (try (tools/patch-resource! opts snapshot [{:op "add" :path "/spec/suspend" :value true}]) nil (catch Exception e e))]
            (is (str/includes? (ex-message e) "edited concurrently"))
            (is (true? (:concurrent-edit (ex-data e))))
            (is (= 1 @patches))))))
    (testing "deletionTimestamp appeared: also a concurrent edit"
      (with-redefs [tools/get-resource (constantly (-> snapshot (assoc-in [:metadata :resourceVersion] "43")
                                                       (assoc-in [:metadata :deletionTimestamp] "now")))
                    tools/run-command (fn [_ _] stale-422)]
        (is (thrown-with-msg? Exception #"edited concurrently"
                              (tools/patch-resource! opts snapshot [{:op "add" :path "/spec/suspend" :value true}])))))
    (testing "any other kubectl error is not retried"
      (let [patches (atom 0) reads (atom 0)]
        (with-redefs [tools/get-resource (fn [_] (swap! reads inc) snapshot)
                      tools/run-command (fn [_ _] (swap! patches inc) {:exit 1 :out "" :err "error: You must be logged in to the server (Unauthorized)"})]
          (let [e (try (tools/patch-resource! opts snapshot [{:op "add" :path "/spec/suspend" :value true}]) nil (catch Exception e e))]
            (is (str/includes? (ex-message e) "Unauthorized"))
            (is (= 1 @patches))
            (is (zero? @reads))))))
    (testing "422 with an unmoved resourceVersion is the patch's own fault, not retried"
      (let [patches (atom 0)]
        (with-redefs [tools/get-resource (constantly snapshot)
                      tools/run-command (fn [_ _] (swap! patches inc) stale-422)]
          (let [e (try (tools/patch-resource! opts snapshot [{:op "add" :path "/spec/suspend" :value true}]) nil (catch Exception e e))]
            (is (str/includes? (ex-message e) "server rejected our request"))
            (is (= 1 @patches))))))
    (testing "a resourceVersion that keeps moving is bounded"
      (let [rv (atom 42) patches (atom 0)]
        (with-redefs [tools/get-resource (fn [_] (assoc-in snapshot [:metadata :resourceVersion] (str (swap! rv inc))))
                      tools/run-command (fn [_ _] (swap! patches inc) stale-422)]
          (let [e (try (with-out-str (tools/patch-resource! opts snapshot [{:op "add" :path "/spec/suspend" :value true}])) nil (catch Exception e e))]
            (is (str/includes? (ex-message e) "kept moving through 5 attempts"))
            (is (= 5 @patches))))))))



(deftest predicates
  (let [ready {:metadata {:generation 2}
               :status {:phase "Ready" :observedGeneration 2 :conditions [{:type "Ready" :status "True"}]}}]
    (is (tools/ready? ready))
    (testing "stale Ready does not authorize anything"
      (is (not (tools/ready? (assoc-in ready [:status :observedGeneration] 1))))
      (is (not (tools/ready? (assoc-in ready [:status :phase] "Reconciling"))))
      (is (not (tools/ready? (assoc-in ready [:status :conditions 0 :status] "False"))))
      (is (not (tools/ready? nil)))))
  (let [cr {:metadata {:generation 2} :spec {:suspend true} :status {:phase "Suspended" :observedGeneration 2}}]
    (is (tools/acknowledged-suspension? cr))
    (testing "a suspend request alone does not authorize rehearsal"
      (is (not (tools/acknowledged-suspension? (assoc-in cr [:status :observedGeneration] 1))))
      (is (not (tools/acknowledged-suspension? (assoc-in cr [:spec :suspend] false))))
      (is (not (tools/acknowledged-suspension? (assoc-in cr [:status :phase] "Ready"))))
      (is (not (tools/acknowledged-suspension? (assoc-in cr [:metadata :deletionTimestamp] "now"))))))
  (is (str/includes? (tools/phase-line {:metadata {:generation 3} :spec {:suspend true}
                                        :status {:phase "Suspended" :observedGeneration 3
                                                 :conditions [{:type "Ready" :reason "Suspended"}]}})
                     "phase=Suspended reason=Suspended generation=3 observed=3 suspended=true"))
  (is (tools/instant-after? "2026-09-16T10:00:00.500Z" "2026-09-16T10:00:00Z"))
  (is (not (tools/instant-after? "2026-09-16T10:00:00Z" "2026-09-16T10:00:00.500Z")))
  (is (not (tools/instant-after? nil "2026-09-16T10:00:00Z"))))

(deftest deletion-requires-owned-nonworker-identity
  (let [probe {:providerId "123" :profile "redis-dev" :name "redis-dev" :ip "1.2.3.4"}
        droplet {:id 123 :name "redis-dev" :tags [] :networks {:v4 [{:type "public" :ip_address "1.2.3.4"}
                                                                   {:type "private" :ip_address "10.0.0.4"}]}}]
    (is (true? (tools/owned-droplet! droplet probe "redis-dev" #{"456"})))
    (doseq [[label changed probe' workers]
            [["id mismatch" (assoc droplet :id 456) probe #{}]
             ["missing droplet" nil probe #{}]
             ["name mismatch" (assoc droplet :name "foreign") probe #{}]
             ["probe name mismatch" droplet (assoc probe :name "foreign") #{}]
             ["probe profile mismatch" droplet (assoc probe :profile "other") #{}]
             ["worker id" droplet probe #{"123"}]
             ["k8s tag" (assoc droplet :tags ["k8s:worker"]) probe #{}]
             ["ip mismatch" droplet (assoc probe :ip "5.6.7.8") #{}]
             ["private ip only" (assoc-in droplet [:networks :v4] [{:type "private" :ip_address "1.2.3.4"}]) probe #{}]
             ["non-numeric id" droplet (assoc probe :providerId "123; rm") #{}]]]
      (let [e (try (tools/owned-droplet! changed probe' "redis-dev" workers) nil (catch Exception e e))]
        (is (some? e) label)
        (is (tools/fatal? e) label)))))

(deftest failures-are-listed-inside-the-pod-never-read
  (let [seen (atom nil)]
    (with-redefs [tools/run-command (fn [args _] (reset! seen args) {:exit 0 :out "20260916T100000Z-redis-ansible.log\n20260916T110000Z-redis-ansible.log\n" :err ""})]
      (is (= {:count 2 :newest "20260916T110000Z-redis-ansible.log"} (tools/failures opts)))
      (is (= "exec" (nth @seen 3)))
      (is (str/includes? (last @seen) "/data/work/redis-dev/failures"))
      (is (not-any? #(= "cat" %) @seen)))
    (with-redefs [tools/run-command (fn [_ _] {:exit 0 :out "" :err ""})]
      (is (= {:count 0 :newest nil} (tools/failures opts))))
    (with-redefs [tools/run-command (fn [_ _] {:exit 1 :out "" :err "Unauthorized"})]
      (is (str/includes? (:error (tools/failures opts)) "Unauthorized")))))

(defn pod-json [name uid phase start & [deleting?]]
  (cond-> {:metadata {:name name :uid uid} :status {:phase phase :startTime start}}
    deleting? (assoc-in [:metadata :deletionTimestamp] "now")))

(deftest probe-waits-for-the-new-controller-to-report-up
  (with-redefs [tools/sleep! (fn [_] nil)]
    (let [calls (atom []) logs-served (atom 0)
          run (fn [args _]
                (swap! calls conj args)
                (cond
                  (some #{"pods"} args) {:exit 0 :err "" :out (json/generate-string {:items [(pod-json "op-new" "u2" "Running" "2026-09-16T07:57:05Z")]})}
                  (some #{"logs"} args) {:exit 0 :err "" :out (if (< (swap! logs-served inc) 3)
                                                                "Clojure tools not yet in expected location\nDownloading clojure-tools.zip\n"
                                                                "Downloading clojure-tools.zip\nRedisDeployment controller running\n")}
                  (some #{"exec"} args) {:exit 0 :err "" :out "{\"healthy\":true,\"providerId\":\"1\"}"}))]
      (with-redefs [tools/run-command run]
        (let [result (with-out-str (is (= {:healthy true :providerId "1"} (tools/probe opts "health"))))]
          (is (str/includes? result "still starting"))))
      (testing "no exec happened before the ready line was served"
        (let [first-exec (.indexOf (mapv #(boolean (some #{"exec"} %)) @calls) true)
              third-logs (nth (keep-indexed (fn [i args] (when (some #{"logs"} args) i)) @calls) 2)]
          (is (= 3 (count (filter #(some #{"logs"} %) @calls))))
          (is (> first-exec third-logs))))
      (testing "logs are read from the pod itself since its own start time"
        (let [logs (first (filter #(some #{"logs"} %) @calls))]
          (is (some #{"pod/op-new"} logs))
          (is (some #{"--since-time=2026-09-16T07:57:05Z"} logs)))))
    (testing "a terminating pod beside the new one, or a pod not yet Running, is not a controller"
      (with-redefs [tools/kubectl (fn [_ args & _] {:items [(pod-json "old" "u1" "Running" "t0" true) (pod-json "new" "u2" "Running" "t1")]})]
        (is (= {:name "new" :uid "u2" :start-time "t1"} (tools/controller-pod opts))))
      (with-redefs [tools/kubectl (fn [_ args & _] {:items [(pod-json "old" "u1" "Running" "t0") (pod-json "new" "u2" "Running" "t1")]})]
        (is (nil? (tools/controller-pod opts))))
      (with-redefs [tools/kubectl (fn [_ args & _] {:items [(pod-json "new" "u2" "Pending" nil)]})]
        (is (nil? (tools/controller-pod opts)))))
    (testing "a pod that never reports up is a bounded error, and nothing is exec'd"
      (let [execs (atom 0)]
        (with-redefs [tools/kubectl (fn [_ args & _]
                                      (cond (some #{"pods"} args) {:items [(pod-json "new" "u2" "Running" "t1")]}
                                            (some #{"logs"} args) "Downloading clojure-tools.zip"
                                            :else (swap! execs inc)))]
          (let [e (try (with-out-str (tools/controller-up! opts {:timeout-ms 1})) nil (catch Exception e e))]
            (is (str/includes? (ex-message e) "controller pod to report"))
            (is (zero? @execs))))))))
