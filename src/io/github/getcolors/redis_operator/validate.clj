(ns io.github.getcolors.redis-operator.validate
  "Desired-state validation for the launcher side of the package. Every error
  is collected so one run reports them all; the launcher exits 2 on any."
  (:require [clojure.string :as str]
            [green.cli :as green-cli]
            [io.github.getcolors.redis.validate :as redis-validate]
            [io.github.getcolors.redis.workflow :as redis-workflow]))

(def profile-par (green-cli/par-name :profile))

(defn env-errors [env]
  (when (not-empty (str (get env profile-par)))
    [(str profile-par " is set; profile must come from colors.yml only")]))

(def config-keys
  "The `spec.config` block of the custom resource, in CRD order. `profile` is
  added from the top-level key at render time and is not listed here."
  [:provider-compute :provider-backend
   :redis-image :redis-port
   :redis-backup-r2-bucket :redis-backup-r2-endpoint :redis-backup-r2-region
   :redis-backup-oncalendar :redis-backup-retention-days :redis-backup-max-age-hours
   :digitalocean-region :digitalocean-size :digitalocean-image :digitalocean-ssh-sources
   :r2-bucket :r2-endpoint])

(def required
  (into [:profile :kube-context :namespace :resource-name :image
         :reconcile-interval :deletion-policy :compute-prevent-destroy]
        config-keys))

(defn missing? [x] (or (nil? x) (and (string? x) (str/blank? x))))

(def profile-re #"[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}")
(def dns-label-re #"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
(def dns-subdomain-re #"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?")
(def image-re #"[^\s]+@sha256:[a-f0-9]{64}")
(def interval-re #"[1-9][0-9]*(ms|s|m|h)")
(def context-re #"[A-Za-z0-9][A-Za-z0-9._:@/-]{0,252}")
(def uuid-re #"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")

(defn- octets [address]
  (let [parts (str/split address #"\.")]
    (when (and (= 4 (count parts)) (every? #(re-matches #"(?:0|[1-9][0-9]{0,2})" %) parts))
      (let [values (mapv parse-long parts)]
        (when (every? #(<= 0 % 255) values) values)))))

(defn- in-block? [[a b c d] [base bits]]
  (let [ip (bit-or (bit-shift-left a 24) (bit-shift-left b 16) (bit-shift-left c 8) d)
        [ba bb bc bd] base
        net (bit-or (bit-shift-left ba 24) (bit-shift-left bb 16) (bit-shift-left bc 8) bd)
        mask (bit-and 0xFFFFFFFF (bit-shift-left 0xFFFFFFFF (- 32 bits)))]
    (= (bit-and ip mask) (bit-and net mask))))

(def non-global-blocks
  "What Python's ipaddress.is_global rejects for IPv4: private, loopback,
  link-local, shared address space, documentation, benchmarking, reserved,
  and the broadcast address."
  [[[0 0 0 0] 8] [[10 0 0 0] 8] [[100 64 0 0] 10] [[127 0 0 0] 8]
   [[169 254 0 0] 16] [[172 16 0 0] 12] [[192 0 0 0] 24] [[192 0 2 0] 24]
   [[192 168 0 0] 16] [[198 18 0 0] 15] [[198 51 100 0] 24] [[203 0 113 0] 24]
   [[224 0 0 0] 4] [[240 0 0 0] 4] [[255 255 255 255] 32]])

(defn public-ipv4-host?
  "One public IPv4 address written as a /32 network, exactly what install.py
  accepted: `a.b.c.d/32`, globally routable."
  [value]
  (boolean
   (when (string? value)
     (when-let [[_ address] (re-matches #"([0-9.]+)/32" value)]
       (when-let [parts (octets address)]
         (not-any? #(in-block? parts %) non-global-blocks))))))

(defn ssh-source-errors [sources]
  (cond
    (missing? sources) []
    (not (sequential? sources)) [":digitalocean-ssh-sources must be a list"]
    (empty? sources) [":digitalocean-ssh-sources must list at least one address"]
    :else (for [source sources :when (not (public-ipv4-host? source))]
            (str ":digitalocean-ssh-sources entry " (pr-str source)
                 " must be a public IPv4 /32 network"))))

(defn config
  "The `spec.config` block, shaped for the custom resource: the listed keys,
  plus `profile` from the top level."
  [opts]
  (into {:profile (:profile opts)}
        (for [k config-keys :when (contains? opts k)] [k (get opts k)])))

(defn- package-shape
  "The config block as the in-cluster adapter merges it before handing it to
  the Redis package, minus credentials, so the package's own validators judge
  the same map the controller will."
  [opts]
  (merge redis-workflow/defaults (config opts)
         {:workdir "/data/work" :compute-prevent-destroy true
          :redis-storage-managed false
          :provider-compute "digitalocean" :provider-backend "r2"}))

(defn state-errors [opts]
  (vec
   (concat
    (for [k required :when (missing? (get opts k))] (str k " is required"))
    (when-not (or (missing? (:profile opts)) (re-matches profile-re (str (:profile opts))))
      [":profile must match [a-zA-Z0-9][a-zA-Z0-9._-]{0,127}"])
    (when-not (or (missing? (:kube-context opts)) (re-matches context-re (str (:kube-context opts))))
      [":kube-context must be a kubectl context name"])
    (when-not (or (missing? (:namespace opts)) (re-matches dns-label-re (str (:namespace opts))))
      [":namespace must be a DNS label"])
    (when-not (or (missing? (:resource-name opts)) (re-matches dns-label-re (str (:resource-name opts))))
      [":resource-name must be a DNS label (defaults to profile)"])
    (when-not (or (missing? (:image opts)) (re-matches image-re (str (:image opts))))
      [":image must be an immutable reference: <repository>@sha256:<64 hex digits>"])
    (when-not (or (nil? (:image-pull-secret opts)) (re-matches dns-subdomain-re (str (:image-pull-secret opts))))
      [":image-pull-secret must be the DNS name of an existing Secret in the namespace"])
    (when-not (or (missing? (:reconcile-interval opts)) (re-matches interval-re (str (:reconcile-interval opts))))
      [":reconcile-interval must be a positive duration in ms, s, m or h"])
    (when-not (or (missing? (:deletion-policy opts)) (contains? #{"Retain" "Destroy"} (:deletion-policy opts)))
      [":deletion-policy must be Retain or Destroy"])
    (when-not (boolean? (:compute-prevent-destroy opts))
      [":compute-prevent-destroy must be true or false"])
    (when-not (or (nil? (:doks-cluster-id opts)) (re-matches uuid-re (str (:doks-cluster-id opts))))
      [":doks-cluster-id must be a DigitalOcean Kubernetes cluster UUID"])
    (when-not (or (missing? (:provider-compute opts)) (= "digitalocean" (:provider-compute opts)))
      [":provider-compute must be digitalocean"])
    (when-not (or (missing? (:provider-backend opts)) (= "r2" (:provider-backend opts)))
      [":provider-backend must be r2"])
    (ssh-source-errors (:digitalocean-ssh-sources opts))
    ;; The package's validators see the config block exactly as the controller
    ;; will hand it to the Redis workflow: image digest, port range, backup
    ;; URL and integers, and the R2 backend plan.
    (remove #(re-find #"is required$" %) (redis-validate/state-errors (package-shape opts))))))

(def credentials
  "The five variables the controller's `credential-vars` expects, copied into
  the redis-credentials Secret by `create`. Names only; values never render."
  ["COLORS_PAR_DO_TOKEN" "COLORS_PAR_R2_ACCESS_KEY_ID" "COLORS_PAR_R2_SECRET_ACCESS_KEY"
   "COLORS_PAR_REDIS_BACKUP_R2_ACCESS_KEY_ID" "COLORS_PAR_REDIS_BACKUP_R2_SECRET_ACCESS_KEY"])

(defn credential-errors [env]
  (for [k credentials :when (str/blank? (get env k))]
    (str "required credential is not set: " k)))

(def drill-par "COLORS_PAR_DRILL_DELETE_OWNED_DROPLET")

(defn drill-errors
  "`drill` deletes a live Droplet. It runs only under an explicit, exact
  acknowledgement that is not part of desired state."
  [env]
  (when-not (= "true" (get env drill-par))
    [(str "drill deletes the owned Redis Droplet to prove recovery; set " drill-par
          "=true for one run to acknowledge that")]))

(defn destroy-errors [opts]
  (when-not (false? (:compute-prevent-destroy opts))
    [(str "compute destruction is protected; set "
          (green-cli/par-name :compute-prevent-destroy) "=false to delete")]))
