(ns colors.redis-test
  (:require [babashka.fs :as fs]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [colors.redis :as redis])
  (:import [java.nio.file Files LinkOption]
           [java.nio.file.attribute PosixFilePermissions]
           [java.time ZoneOffset ZonedDateTime]))
(def config {:profile "redis-test" :r2-bucket "redis-state" :r2-endpoint "https://example.test"
             :digitalocean-region "ams3" :digitalocean-size "s-1vcpu-2gb"
             :digitalocean-image "ubuntu-24-04-x64"})
(def node {:provider_id "123" :name "redis-test" :ip "192.0.2.10"})
(def droplet {:id 123 :name "redis-test" :status "active" :region {:slug "ams3"}
              :size_slug "s-1vcpu-2gb" :image {:slug "ubuntu-24-04-x64"}
              :networks {:v4 [{:type "public" :ip_address "192.0.2.10"}]}})
(def dependencies {:inspect (constantly {:status "present" :cluster {:nodes [node]}})
                   :provider-get (fn [_ _] droplet) :service-health (constantly true)
                   :read-marker (constantly {:config-hash (redis/config-hash config) :provider-id "123"})})
(deftest observation-boundaries
  (is (= {:exists? true :matches? true :ready? true} (redis/observe config dependencies)))
  (testing "Confirmed provider absence requests repair"
    (is (= {:exists? false :matches? false :ready? false}
           (redis/observe config (assoc dependencies :provider-get (constantly nil))))))
  (testing "Transport errors never become absence"
    (is (thrown? Exception (redis/observe config (assoc dependencies :provider-get (fn [& _] (throw (Exception. "401")))))))
    (is (thrown? Exception (redis/observe config (assoc dependencies :inspect (constantly {:status "error"}))))))
  (testing "Unhealthy existing Redis requests convergence, never absence"
    (is (= {:exists? true :matches? false :ready? false}
           (redis/observe config (assoc dependencies :service-health (constantly false))))))
  (testing "Configuration and provider changes invalidate success"
    (is (false? (:matches? (redis/observe (assoc config :redis-port 6380) dependencies))))
    (is (false? (:matches? (redis/observe config (assoc dependencies :provider-get (constantly (assoc droplet :size_slug "other"))))))))
  (testing "Initial partial compute can resume without treating it as absent"
    (is (= {:exists? true :matches? false :ready? false}
           (redis/observe config (assoc dependencies :inspect (constantly {:status "partial"}))))))
  (testing "Missing Droplet still requires shared-resource deletion"
    (is (:exists? (redis/observe (assoc config :green/event :delete) (assoc dependencies :provider-get (constantly nil)))))))
(deftest guards
  (is (= ["r2" "https://example.test" "redis-state" "redis-test"] (redis/identity config)))
  (is (true? (:compute-prevent-destroy (redis/options (assoc config :compute-prevent-destroy false)))))
  (is (thrown? Exception (redis/delete config)))
  (is (thrown? Exception (redis/check-environment! {"COLORS_PAR_PROFILE" "override"}))))

(deftest new-provider-id-invalidates-marker
  (let [changed (assoc node :provider_id "456")]
    (is (= {:exists? true :matches? false :ready? true}
           (redis/observe config (assoc dependencies :inspect
                                       (constantly {:status "present" :cluster {:nodes [changed]}})))))))

(deftest provider-response-boundaries
  (require '[babashka.http-client :as http])
  (with-redefs [babashka.http-client/get (fn [& _] {:status 404})]
    (is (nil? (redis/provider-get {:do-token "secret"} node))))
  (doseq [status [401 403 429 500]]
    (with-redefs [babashka.http-client/get (fn [& _] {:status status :body "secret"})]
      (try (redis/provider-get {:do-token "secret"} node)
           (is false "Expected an observation error")
           (catch Exception e
             (is (= {:status status} (ex-data e)))
             (is (not (.contains (ex-message e) "secret")))))))
  (with-redefs [babashka.http-client/get (fn [& _] {:status 200 :body "{\"droplet\":{\"id\":123,\"name\":\"foreign\"}}"})]
    (is (thrown? Exception (redis/provider-get {} node)))))

(deftest workflow-boundaries
  (let [seen (atom []) marked (atom false) retained (atom nil)]
    (with-redefs [green.workflow/run (fn [_ opts] (swap! seen conj opts) {:green/exit 1 :green/err "secret" :do-token "secret" :green/step :redis/ansible})
                  redis/write-marker! (fn [& _] (reset! marked true))
                  redis/retain-failure! (fn [_ result & _] (reset! retained result) "retained.log")]
      (let [out (with-out-str (is (= {:green/exit 1} (redis/converge config))))]
        (is (true? (:compute-prevent-destroy (first @seen))))
        (is (false? @marked))
        (testing "the failure is retained for diagnosis, and the log line names the file, not the error"
          (is (= :redis/ansible (:green/step @retained)))
          (is (str/includes? out "converge outcome=failed step=:redis/ansible exit=1 retained=retained.log"))
          (is (not (str/includes? out "secret"))))))
    (reset! seen [])
    (with-redefs [redis/inspect (constantly {:status "absent"})
                  green.workflow/run (fn [_ opts] (swap! seen conj opts) {:green/exit 0})]
      (is (thrown? Exception (redis/delete config)))
      (is (empty? @seen))
      (is (= {:green/exit 0} (redis/delete (assoc config :green.kubernetes/resource {:spec {:deletionPolicy "Destroy"}}))))
      (is (false? (:compute-prevent-destroy (first @seen))))
      (is (= :delete (:green/event (first @seen)))))))

(deftest secrets-are-masked-in-failure-reports
  (let [env {"COLORS_PAR_DO_TOKEN" "dop_v1_abc" "COLORS_PAR_R2_SECRET_ACCESS_KEY" "abc" "PATH" "/usr/bin" "COLORS_PAR_EMPTY" ""}]
    (is (= "token *** key *** path /usr/bin ***" (redis/mask "token dop_v1_abc key abc path /usr/bin dop_v1_abc" env)))
    (testing "the longer value is masked whole even though it contains the shorter one"
      (is (= "***" (redis/mask "dop_v1_abc" env))))
    (is (= "nothing" (redis/mask "nothing" {})))
    (let [report (redis/failure-report {:profile "p" :green/event :create}
                                       {:green/step :redis/ansible :green/exit 2
                                        :green/err "ansible-playbook main.yml failed: Bearer dop_v1_abc rejected"
                                        :ansible/recap {:host {:failed 1}}
                                        :green/trace "at line 1 dop_v1_abc"}
                                       env)]
      (is (not (str/includes? report "dop_v1_abc")))
      (doseq [line ["profile: p" "event: create" "step: :redis/ansible" "exit: 2" "err:" "Bearer *** rejected" "recap:" ":failed 1" "trace:"]]
        (is (str/includes? report line) line)))))

(deftest failure-logs-are-private-and-rotated
  (let [dir (fs/create-temp-dir)]
    (try
      (with-redefs [redis/options (fn [config] {:workdir (str dir) :profile (:profile config)})]
        (let [config {:profile "redis-test" :green/event :create}
              at (fn [i] (.plusSeconds (ZonedDateTime/of 2026 9 16 10 0 0 0 ZoneOffset/UTC) i))
              names (doall (for [i (range 23)]
                             (redis/retain-failure! config {:green/step :redis/ansible :green/exit 2 :green/err (str "failure " i " dop_v1_abc")}
                                                    {"COLORS_PAR_DO_TOKEN" "dop_v1_abc"} (at i))))
              failures (fs/file dir "redis-test" "failures")
              kept (sort (map fs/file-name (fs/list-dir failures)))]
          (is (= "20260916T100000Z-redis-ansible.log" (first names)))
          (is (= 20 (count kept)))
          (is (= (drop 3 names) kept))
          (is (= "rwx------" (PosixFilePermissions/toString (Files/getPosixFilePermissions (.toPath failures) (make-array LinkOption 0)))))
          (let [newest (fs/file failures (last kept)) content (slurp newest)]
            (is (= "rw-------" (PosixFilePermissions/toString (Files/getPosixFilePermissions (.toPath newest) (make-array LinkOption 0)))))
            (is (str/includes? content "failure 22 ***"))
            (is (not (str/includes? content "dop_v1_abc"))))
          (is (not-any? #(str/ends-with? % ".tmp") kept))))
      (finally (fs/delete-tree dir)))))
