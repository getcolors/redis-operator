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
