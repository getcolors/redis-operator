(ns colors.main
  (:require [colors.redis :as redis]
            [green.kubernetes :as k8s]
            [green.kubernetes.client :as client]))
(defn -main [& [context namespace]]
  (when-not context (throw (ex-info "Usage: bb controller <context|--in-cluster> [namespace]" {})))
  (redis/check-environment! (into {} (System/getenv)))
  (let [transport (client/kubectl-client (if (= context "--in-cluster")
                                         {:in-cluster? true} {:context context}))
        runtime (k8s/start! (k8s/controller {:packages [(redis/package)] :client transport
                                           :namespace (or namespace "colors-dev")
                                           :workers 1 :poll-ms 2000}))
        stopped (promise)]
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. (fn [] (k8s/stop! runtime) (deliver stopped true))))
    (println "RedisDeployment controller running")
    @stopped))
