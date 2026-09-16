(ns io.github.getcolors.redis-operator.validate-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [green.cli :as green-cli]
            [io.github.getcolors.redis-operator.validate :as validate]
            [io.github.getcolors.redis-operator.workflow :as workflow]))

(def fixture
  (-> (green-cli/read-state "colors.yml" (slurp "test/fixtures/colors.yml"))
      (assoc :workdir ".colors" :resource-name "redis-operator-fixture")))

(def valid (merge workflow/defaults fixture))

(defn errors [overrides] (validate/state-errors (merge valid overrides)))

(defn has-error? [errors fragment] (boolean (some #(str/includes? % fragment) errors)))

(deftest fixture-is-valid
  (is (= [] (errors {})))
  (is (nil? (validate/env-errors {}))))

(deftest every-error-is-reported-at-once
  (let [e (errors {:kube-context nil :image "redis-operator:latest" :deletion-policy "Purge"})]
    (is (has-error? e ":kube-context is required"))
    (is (has-error? e ":image must be an immutable reference"))
    (is (has-error? e ":deletion-policy must be Retain or Destroy"))
    (is (= 3 (count e)))))

(deftest each-key-is-validated
  (testing "profile"
    (is (has-error? (errors {:profile nil}) ":profile is required"))
    (is (has-error? (errors {:profile "-bad"}) ":profile must match")))
  (testing "kube-context"
    (is (has-error? (errors {:kube-context "bad context"}) ":kube-context must be a kubectl context name")))
  (testing "namespace and resource-name are DNS labels"
    (is (has-error? (errors {:namespace "Colors.Redis"}) ":namespace must be a DNS label"))
    (is (has-error? (errors {:resource-name "redis.dev"}) ":resource-name must be a DNS label")))
  (testing "image must be digest-pinned"
    (is (has-error? (errors {:image "registry.example/redis-operator:v1"}) ":image must be an immutable reference"))
    (is (has-error? (errors {:image "registry.example/redis-operator@sha256:abc"}) ":image must be an immutable reference"))
    (is (= [] (errors {:image (str "registry.example/redis-operator@sha256:" (apply str (repeat 64 "f")))}))))
  (testing "image-pull-secret is optional but well formed"
    (is (= [] (errors {:image-pull-secret nil})))
    (is (has-error? (errors {:image-pull-secret "Not Valid"}) ":image-pull-secret must be")))
  (testing "reconcile-interval matches the CRD pattern"
    (is (has-error? (errors {:reconcile-interval "0s"}) ":reconcile-interval must be"))
    (is (has-error? (errors {:reconcile-interval "5 minutes"}) ":reconcile-interval must be"))
    (is (= [] (errors {:reconcile-interval "500ms"}))))
  (testing "compute-prevent-destroy is a boolean"
    (is (has-error? (errors {:compute-prevent-destroy "yes"}) ":compute-prevent-destroy must be true or false")))
  (testing "doks-cluster-id is an optional UUID"
    (is (= [] (errors {:doks-cluster-id "a87775cd-de9f-4390-8dee-281f864bc9de"})))
    (is (has-error? (errors {:doks-cluster-id "cluster"}) ":doks-cluster-id must be")))
  (testing "providers are fixed by the CRD"
    (is (has-error? (errors {:provider-compute "vultr"}) ":provider-compute must be digitalocean"))
    (is (has-error? (errors {:provider-backend "s3"}) ":provider-backend must be r2")))
  (testing "the Redis package validators judge the config block"
    (is (has-error? (errors {:redis-image "redis:7.2"}) ":redis-image must be pinned by digest"))
    (is (has-error? (errors {:redis-port 70000}) ":redis-port must be an integer between 1 and 65535"))
    (is (has-error? (errors {:redis-backup-r2-endpoint "http://plain"}) ":redis-backup-r2-endpoint must be an https URL"))
    (is (has-error? (errors {:redis-backup-retention-days 0}) ":redis-backup-retention-days must be a positive integer"))
    (is (has-error? (errors {:redis-backup-r2-region nil}) ":redis-backup-r2-region is required"))
    (is (has-error? (errors {:r2-endpoint nil}) ":r2-endpoint is required"))))

(deftest ssh-sources-are-public-ipv4-hosts
  (is (validate/public-ipv4-host? "93.184.216.34/32"))
  (is (validate/public-ipv4-host? "209.38.46.78/32"))
  (doseq [bad ["10.0.0.1/32" "192.168.1.1/32" "172.16.0.1/32" "127.0.0.1/32" "169.254.1.1/32"
               "100.64.0.1/32" "192.0.2.1/32" "198.51.100.1/32" "203.0.113.1/32" "224.0.0.1/32"
               "240.0.0.1/32" "255.255.255.255/32" "0.0.0.0/32"
               "93.184.216.34" "93.184.216.34/24" "93.184.216.0/32 " "::1/128" "999.1.1.1/32" "a.b.c.d/32"]]
    (is (not (validate/public-ipv4-host? bad)) bad))
  (is (has-error? (errors {:digitalocean-ssh-sources []}) "at least one address"))
  (is (has-error? (errors {:digitalocean-ssh-sources "93.184.216.34/32"}) "must be a list"))
  (is (has-error? (errors {:digitalocean-ssh-sources ["93.184.216.34/32" "10.0.0.1/32"]})
                  "\"10.0.0.1/32\" must be a public IPv4 /32")))

(deftest config-block-carries-profile-and-only-crd-keys
  (let [config (validate/config (assoc valid :kube-context "ctx" :image "x"))]
    (is (= (:profile valid) (:profile config)))
    (is (not (contains? config :kube-context)))
    (is (not (contains? config :image)))
    (is (not (contains? config :compute-prevent-destroy)))
    (is (= (set (conj validate/config-keys :profile)) (set (keys config))))))

(deftest guards
  (testing "profile override is refused"
    (is (= 1 (count (validate/env-errors {"COLORS_PAR_PROFILE" "other"})))))
  (testing "create needs the five credentials"
    (is (= 5 (count (validate/credential-errors {}))))
    (is (= 1 (count (validate/credential-errors (zipmap (butlast validate/credentials) (repeat "x"))))))
    (is (empty? (validate/credential-errors (zipmap validate/credentials (repeat "x"))))))
  (testing "drill runs only under the exact acknowledgement"
    (is (seq (validate/drill-errors {})))
    (is (seq (validate/drill-errors {validate/drill-par "True"})))
    (is (seq (validate/drill-errors {validate/drill-par "yes"})))
    (is (nil? (validate/drill-errors {validate/drill-par "true"}))))
  (testing "delete needs the guard lifted"
    (is (seq (validate/destroy-errors {:compute-prevent-destroy true})))
    (is (seq (validate/destroy-errors {})))
    (is (nil? (validate/destroy-errors {:compute-prevent-destroy false})))))
