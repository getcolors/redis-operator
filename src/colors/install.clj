(ns colors.install
  "Render the operator installation and optionally apply it through Green."
  (:require [cheshire.core :as json]
            [green.cli :as cli]
            [green.process :as process]
            [green.workflow :as wf]
            [clojure.java.io :as io]))

(defn manifests [namespace image]
  (when-not (and (re-matches #"[a-z0-9][a-z0-9-]{0,61}[a-z0-9]|[a-z0-9]" namespace)
                 (re-matches #"[^\s]+@sha256:[a-f0-9]{64}" image))
    (throw (ex-info "Expected namespace and immutable image digest" {})))
  (let [name "colors-redis-operator"
        metadata {:name name :namespace namespace}
        labels {:app name}]
    [{:apiVersion "v1" :kind "Namespace" :metadata {:name namespace}}
     (cli/read-state "manifests/crd.yml" (slurp "manifests/crd.yml"))
     {:apiVersion "v1" :kind "ServiceAccount" :metadata metadata}
     {:apiVersion "rbac.authorization.k8s.io/v1" :kind "Role" :metadata metadata
      :rules [{:apiGroups ["colors.getcolors.ai"]
               :resources ["redisdeployments" "redisdeployments/status" "redisdeployments/finalizers"]
               :verbs ["get" "list" "watch" "patch" "update"]}]}
     {:apiVersion "rbac.authorization.k8s.io/v1" :kind "RoleBinding" :metadata metadata
      :subjects [{:kind "ServiceAccount" :name name :namespace namespace}]
      :roleRef {:apiGroup "rbac.authorization.k8s.io" :kind "Role" :name name}}
     {:apiVersion "v1" :kind "PersistentVolumeClaim" :metadata metadata
      :spec {:accessModes ["ReadWriteOnce"] :resources {:requests {:storage "5Gi"}}}}
     {:apiVersion "apps/v1" :kind "Deployment" :metadata metadata
      :spec {:replicas 1 :strategy {:type "Recreate"} :selector {:matchLabels labels}
             :template {:metadata {:labels labels}
                        :spec {:serviceAccountName name :terminationGracePeriodSeconds 7500
                               :imagePullSecrets [{:name "registry-credentials"}]
                               :containers [{:name "controller" :image image :args [namespace]
                                             :envFrom [{:secretRef {:name "redis-credentials"}}]
                                             :resources {:requests {:cpu "100m" :memory "256Mi"}
                                                         :limits {:memory "2Gi"}}
                                             :volumeMounts [{:name "state" :mountPath "/data"}
                                                            {:name "state" :mountPath "/root/.ssh" :subPath "ssh"}]}]
                               :volumes [{:name "state" :persistentVolumeClaim {:claimName name}}]}}}}]))

(defn -main [namespace image]
  (println (json/generate-string {:apiVersion "v1" :kind "List" :items (manifests namespace image)})))

(defn install-step [opts]
  (let [file (java.io.File/createTempFile "colors-redis-install-" ".json")]
    (try
      (spit file (json/generate-string {:apiVersion "v1" :kind "List"
                                       :items (manifests (:namespace opts) (:image opts))}))
      (let [result (process/run-with-timeout
                    ["kubectl" "--context" (:context opts) "apply" "-f" (.getPath file)] {} 120000)]
        (assoc opts :green/exit (if (zero? (:exit result)) 0 1)))
      (finally (.delete file)))))

(defn ready-step [opts]
  (let [result (process/run-with-timeout
                ["kubectl" "--context" (:context opts) "rollout" "status"
                 "deployment/colors-redis-operator" "-n" (:namespace opts) "--timeout=600s"] {} 610000)]
    (assoc opts :green/exit (if (zero? (:exit result)) 0 1))))

(def workflow (wf/workflow {:start :install
                            :wire-fn (fn [step _]
                                       (case step :install [install-step :ready] :ready [ready-step]))}))

(defn install! [context namespace image]
  (let [result (wf/run workflow {:green/event :create :context context
                                 :namespace namespace :image image})]
    (if (wf/failed? result)
      (throw (ex-info "Operator installation did not become ready" {}))
      {:green/exit 0})))
