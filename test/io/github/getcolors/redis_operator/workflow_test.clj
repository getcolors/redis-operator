(ns io.github.getcolors.redis-operator.workflow-test
  (:require [babashka.fs :as fs]
            [cheshire.core :as json]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [green.cli :as green-cli]
            [green.workflow :as wf]
            [io.github.getcolors.redis-operator.operator :as operator]
            [io.github.getcolors.redis-operator.tools :as tools]
            [io.github.getcolors.redis-operator.workflow :as workflow]))

(def fixture (green-cli/read-state "colors.yml" (slurp "test/fixtures/colors.yml")))

(defn opts [tmp & [overrides]]
  (merge (assoc fixture :workdir (str tmp) :green/state-file (str (fs/path tmp "colors.yml")))
         overrides))

(def credentials (zipmap ["COLORS_PAR_DO_TOKEN" "COLORS_PAR_R2_ACCESS_KEY_ID" "COLORS_PAR_R2_SECRET_ACCESS_KEY"
                          "COLORS_PAR_REDIS_BACKUP_R2_ACCESS_KEY_ID" "COLORS_PAR_REDIS_BACKUP_R2_SECRET_ACCESS_KEY"]
                         (repeat "secret-value")))

(defmacro with-tmp [[sym] & body]
  `(let [~sym (fs/create-temp-dir)]
     (try ~@body (finally (fs/delete-tree ~sym)))))

(defn no-kubectl [& _] (throw (ex-info "kubectl must not run" {})))

(deftest build-renders-without-contact
  (with-tmp [tmp]
    (with-redefs [tools/run-command no-kubectl]
      (let [result (wf/run workflow/workflow (assoc (opts tmp) :green/event :build))]
        (is (zero? (:green/exit result)) (:green/err result))
        (is (fs/exists? (fs/path tmp "redis-operator-fixture" "operator" "manifests.json")))
        (let [cr (json/parse-string (slurp (str (fs/path tmp "redis-operator-fixture" "operator" "redis-deployment.json"))) true)]
          (is (= "redis-operator-fixture" (get-in cr [:metadata :name])))
          (is (= "redis-operator-fixture" (get-in cr [:spec :config :profile]))))))))

(deftest dry-run-create-skips-every-side-effect
  (with-tmp [tmp]
    (with-redefs [tools/run-command no-kubectl]
      (let [out (with-out-str
                  (let [result (wf/run workflow/workflow (assoc (opts tmp) :green/event :create :green/dry-run true))]
                    (is (zero? (:green/exit result)) (:green/err result))))]
        (doseq [step ["namespace" "credentials" "pull-secret" "install" "resource" "ready"]]
          (is (str/includes? out (str "dry-run: would run :redis-operator/" step))))))))

(deftest preflight-guards
  (with-tmp [tmp]
    (testing "real create needs the five credentials and nothing else from the environment"
      (let [result (workflow/start-step (assoc (opts tmp) :green/event :create) {})]
        (is (= 2 (:green/exit result)))
        (is (= 5 (count (str/split-lines (:green/err result))))))
      (is (zero? (:green/exit (workflow/start-step (assoc (opts tmp) :green/event :create) credentials)))))
    (testing "delete refuses under the committed guard, dry-run included"
      (let [result (workflow/start-step (assoc (opts tmp) :green/event :delete :green/dry-run true) {})]
        (is (= 2 (:green/exit result)))
        (is (str/includes? (:green/err result) "COLORS_PAR_COMPUTE_PREVENT_DESTROY=false")))
      (is (zero? (:green/exit (workflow/start-step (assoc (opts tmp) :green/event :delete)
                                                   {"COLORS_PAR_COMPUTE_PREVENT_DESTROY" "false"})))))
    (testing "drill needs the acknowledgement and the DO token"
      (is (= 2 (:green/exit (workflow/start-step (assoc (opts tmp) :green/event :drill) {}))))
      (let [result (workflow/start-step (assoc (opts tmp) :green/event :drill) {"COLORS_PAR_DRILL_DELETE_OWNED_DROPLET" "true"})]
        (is (= 2 (:green/exit result)))
        (is (str/includes? (:green/err result) "COLORS_PAR_DO_TOKEN")))
      (is (zero? (:green/exit (workflow/start-step (assoc (opts tmp) :green/event :drill)
                                                   {"COLORS_PAR_DRILL_DELETE_OWNED_DROPLET" "true" "COLORS_PAR_DO_TOKEN" "t"})))))
    (testing "profile override and resource-name default"
      (is (= 2 (:green/exit (workflow/start-step (assoc (opts tmp) :green/event :build) {"COLORS_PAR_PROFILE" "x"}))))
      (is (= "redis-operator-fixture" (:resource-name (workflow/start-step (assoc (opts tmp) :green/event :build) {})))))))

(deftest credentials-travel-on-stdin-only
  (with-tmp [tmp]
    (let [seen (atom nil)]
      (with-redefs [tools/run-command (fn [args {:keys [input]}] (reset! seen {:args args :input input}) {:exit 0 :out "" :err ""})]
        (workflow/credentials-step (opts tmp) credentials))
      (is (= ["apply" "-f" "-"] (take-last 3 (:args @seen))))
      (is (= credentials (get (json/parse-string (:input @seen)) "stringData")))
      (is (not-any? #(str/includes? (str %) "secret-value") (:args @seen))))
    (testing "an apply failure never echoes the payload"
      (with-redefs [tools/run-command (fn [_ _] {:exit 1 :out "" :err "secret-value rejected"})]
        (let [e (try (workflow/credentials-step (opts tmp) credentials) nil (catch Exception e e))]
          (is (some? e))
          (is (not (str/includes? (ex-message e) "secret-value"))))))))

(deftest pull-secret-wait-distinguishes-absence-from-errors
  (with-tmp [tmp]
    (with-redefs [operator/sleep! (fn [_] nil)]
      (testing "absent then present"
        (let [answers (atom [{:exit 1 :err "Error from server (NotFound): secrets \"colors-fixture\" not found" :out ""}
                             {:exit 0 :err "" :out "secret/colors-fixture\n"}])]
          (with-redefs [tools/run-command (fn [_ _] (let [a (first @answers)] (swap! answers rest) a))]
            (is (zero? (:green/exit (workflow/pull-secret-step (opts tmp))))))))
      (testing "auth errors surface at once with kubectl's text"
        (with-redefs [tools/run-command (fn [_ _] {:exit 1 :out "" :err "error: You must be logged in to the server (Unauthorized)"})]
          (let [e (try (workflow/pull-secret-step (opts tmp)) nil (catch Exception e e))]
            (is (str/includes? (ex-message e) "Unauthorized")))))
      (testing "no pull secret configured means no wait"
        (with-redefs [tools/run-command no-kubectl]
          (is (zero? (:green/exit (workflow/pull-secret-step (dissoc (opts tmp) :image-pull-secret))))))))))

(deftest create-refuses-a-suspended-resource
  (with-tmp [tmp]
    (let [applied (atom [])]
      (with-redefs [tools/get-resource (constantly {:metadata {:generation 4} :spec {:suspend true}})
                    tools/apply! (fn [_ doc & _] (swap! applied conj doc))]
        (let [e (try (workflow/resource-step (opts tmp)) nil (catch Exception e e))]
          (is (= 1 (:green/exit (ex-data e))))
          (is (str/includes? (ex-message e) "suspended"))
          (is (empty? @applied))))
      (with-redefs [tools/get-resource (constantly {:metadata {:generation 4} :spec {:suspend false}})
                    tools/apply! (fn [_ doc & _] (swap! applied conj doc))]
        (workflow/resource-step (opts tmp))
        (is (= 1 (count @applied)))
        (is (not (contains? (:spec (first @applied)) :suspend)))))))

(deftest delete-runs-through-the-finalizer
  (with-tmp [tmp]
    (let [calls (atom []) patches (atom [])
          reads (atom [{:metadata {:generation 2 :resourceVersion "7"} :spec {:deletionPolicy "Retain"} :status {:phase "Ready"}}
                       {:metadata {:generation 3 :deletionTimestamp "now"} :spec {:deletionPolicy "Destroy"} :status {:phase "Deleting"}}
                       nil])]
      (with-redefs [operator/sleep! (fn [_] nil)
                    tools/get-resource (fn [_] (let [r (first @reads)] (swap! reads rest) r))
                    tools/patch-resource! (fn [_ _ patch] (swap! patches conj patch) {})
                    tools/kubectl (fn [_ args & _] (swap! calls conj args) nil)]
        (is (zero? (:green/exit (workflow/destroy-step (opts tmp)))))
        (is (= [[{:op "add" :path "/spec/deletionPolicy" :value "Destroy"}]] @patches))
        (is (= 1 (count (filter #(= "delete" (first %)) @calls))))))
    (testing "a suspended resource is never deleted from under a rehearsal"
      (with-redefs [tools/get-resource (constantly {:metadata {:generation 2} :spec {:suspend true}})
                    tools/kubectl no-kubectl tools/patch-resource! no-kubectl]
        (let [e (try (workflow/destroy-step (opts tmp)) nil (catch Exception e e))]
          (is (str/includes? (ex-message e) "suspended")))))
    (testing "the CRD stays while any RedisDeployment remains"
      (let [calls (atom [])]
        (with-redefs [tools/kubectl (fn [_ args & _] (swap! calls conj args)
                                      (when (= "get" (first args)) {:items [{:metadata {:namespace "other"}}]}))]
          (workflow/crd-delete-step (opts tmp))
          (is (not-any? #(= "delete" (first %)) @calls)))
        (reset! calls [])
        (with-redefs [tools/kubectl (fn [_ args & _] (swap! calls conj args)
                                      (when (= "get" (first args)) {:items []}))]
          (workflow/crd-delete-step (opts tmp))
          (is (some #(= ["delete" "crd" tools/crd-name "--ignore-not-found"] %) @calls)))))))
