(ns databricks.databricks-config
  (:require [org.httpkit.sni-client :as httpkit]
            [selmer.parser :as s-parser]))

(defn base-request [host]
  {:host host
   :timeout (* 30 1000)
   :headers {"Authorization" (str "Bearer " api-token)}})

(def endpoints-request
  {:clusters/create            {:method      :post
                                :uri         "/api/2.0/clusters/create"}
   :clusters/list              {:method      :get
                                :uri         "/api/2.0/clusters/list"}})

(defn with-full-url [{:keys [uri host] :as request-map} context]
  (let [resolved-uri (s-parser/render uri context)]
    (assoc request-map :url (str host resolved-uri))))

(defn call-api!
  [host api-token endpoint context]
  (let [base-request'    (merge (base-request host api-token))
        endpoint-request (get endpoints-request endpoint {})
        request-map'     (merge base-request' endpoint-request context)
        request-map      (with-full-url request-map' context)]
    (-> request-map
        httpkit/req!
        :body)))

(defn create-cluster [token host payload]
  (call-api! host token :clusters/create {:payload payload}))

(defn cluster-list [token host]
  (call-api! host token :clusters/list {}))
