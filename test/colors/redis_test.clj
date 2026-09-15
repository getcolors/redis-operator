(ns colors.redis-test
  (:require [clojure.test :refer [deftest is testing]]
            [colors.redis :as redis]))
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
  (let [seen (atom []) marked (atom false)]
    (with-redefs [green.workflow/run (fn [_ opts] (swap! seen conj opts) {:green/exit 1 :green/err "secret" :do-token "secret"})
                  redis/write-marker! (fn [& _] (reset! marked true))]
      (is (= {:green/exit 1} (redis/converge config)))
      (is (true? (:compute-prevent-destroy (first @seen))))
      (is (false? @marked)))
    (reset! seen [])
    (with-redefs [redis/inspect (constantly {:status "absent"})
                  green.workflow/run (fn [_ opts] (swap! seen conj opts) {:green/exit 0})]
      (is (thrown? Exception (redis/delete config)))
      (is (empty? @seen))
      (is (= {:green/exit 0} (redis/delete (assoc config :green.kubernetes/resource {:spec {:deletionPolicy "Destroy"}}))))
      (is (false? (:compute-prevent-destroy (first @seen))))
      (is (= :delete (:green/event (first @seen)))))))
