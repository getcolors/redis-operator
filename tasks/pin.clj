(ns pin (:require [clojure.java.shell :as sh] [clojure.string :as str]))
(def path "skills/package-redis-operator-green/green")
(def rx #"\(def \^:private redis-operator-sha (nil|\"[0-9a-f]{40}\")\)")
(defn git [& args] (let [{:keys [exit out]} (apply sh/sh "git" args)] (when (zero? exit) (str/trim out))))
(let [dirty (git "status" "--porcelain") sha (git "rev-parse" "HEAD") remotes (git "branch" "-r" "--contains" sha)]
  (cond (seq dirty) (do (binding [*out* *err*] (println "redis-operator working tree is dirty; commit before pinning")) (System/exit 2))
        (not (str/includes? (str remotes) "origin/")) (do (binding [*out* *err*] (println "redis-operator HEAD is not pushed")) (System/exit 2))
        :else (let [s (slurp path) n (str/replace s rx (str "(def ^:private redis-operator-sha \"" sha "\")"))]
                (spit path n)
                (let [red "skills/package-redis-operator-red/red"
                      blue "skills/package-redis-operator-blue/blue"]
                  (spit red (str/replace (slurp red) #"(\"package-redis-operator-red\": )(null|\"[^\"]+\")" (str "$1\"github:getcolors/redis-operator#" sha "\"")))
                  (spit blue (str/replace (slurp blue) #"PIN = (None|\"[a-f0-9]+\")" (str "PIN = \"" sha "\""))))
                (println "pinned redis-operator launcher to" (subs sha 0 7)))))
