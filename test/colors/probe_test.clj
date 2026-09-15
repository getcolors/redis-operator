(ns colors.probe-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.string :as str]
            [colors.probe :as probe]
            [green.process :as process]))

(deftest probe-inputs-cannot-inject-shell
  (is (= "colors:test-123" (probe/safe-token "colors:test-123")))
  (doseq [value [nil "" "bad key" "x;id" "$(id)" "x'y" "x\ny"]]
    (is (thrown? Exception (probe/safe-token value)))))

(deftest password-read-stays-inside-container
  (let [command (atom nil)]
    (with-redefs [probe/remote (fn [_ value] (reset! command value) "PONG")]
      (is (= "PONG" (probe/redis-command {} "PING"))))
    (is (str/includes? @command "docker compose exec -T redis sh -c"))
    (is (str/includes? @command "export REDISCLI_AUTH="))
    (is (str/ends-with? @command "--raw PING'"))
    (is (not (str/includes? @command "-e REDISCLI_AUTH")))))

(deftest rehearsal-requires-acknowledged-current-suspension
  (let [cr {:metadata {:generation 2} :spec {:suspend true}
            :status {:phase "Suspended" :observedGeneration 2}}]
    (is (nil? (probe/require-suspended! cr)))
    (doseq [resource [(assoc-in cr [:spec :suspend] false)
                     (assoc-in cr [:status :phase] "Ready")
                     (assoc-in cr [:status :observedGeneration] 1)
                     (assoc-in cr [:metadata :deletionTimestamp] "now")]]
      (is (thrown? Exception (probe/require-suspended! resource))))))
