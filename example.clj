(ns databricks-lambdas.migrate-clusters
  (:require [databricks.databricks-config :as databricks-config]))

(def primary (System/getenv "PRIMARY"))

(def secondary (System/getenv "SECONDARY"))

(def host-secondary "https://secondary.cloud.databricks.com")

(def host-primary "https://primary.cloud.databricks.com")

(defn get-clusters [host token]
  (databricks-config/cluster-list token host))

(def create-cluster-template
  {:num_workers 0
   :autoscale {:min_workers 0 :max_workers 0}
   :cluster_name ""
   :spark_version ""
   :spark_conf {}
   :aws_attributes {}
   :node_type_id ""
   :driver_node_type_id ""
   :ssh_public_keys []
   :custom_tags {}
   :spark_env_vars {}
   :autotermination_minutes 0
   :enable_elastic_disk false})

(defn generate-cluster-configs [clusters]
  (->> clusters
       (remove #(= "JOB" (:cluster_source %)))
       (mapv (fn [cluster]
               (-> (merge create-cluster-template cluster)
                   (select-keys (keys create-cluster-template)))))))

(defn migrate-clusters [clusters new-account-token host]
  (->> (generate-cluster-configs clusters)
       (map #(databricks-config/create-cluster new-account-token host %))))

(migrate-clusters (get-clusters host-primary primary) secondary host-secondary)