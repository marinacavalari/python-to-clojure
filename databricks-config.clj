(ns databricks.databricks-config
  (:require [org.httpkit.sni-client :as httpkit]
            [selmer.parser :as s-parser]))

(defn base-request [host]
  {:host host
   :timeout (* 30 1000)})

(defn auth [api-token]
  {:headers {"Authorization" (str "Bearer " api-token)}})

(def endpoints-request
  {:clusters/create            {:method      :post
                                :uri         "/api/2.0/clusters/create"
                                :response-fn :cluster_id}
   :clusters/list              {:method      :get
                                :uri         "/api/2.0/clusters/list"
                                :response-fn :clusters}})

(defn with-full-url [{:keys [uri host] :as request-map} context]
  (let [resolved-uri (s-parser/render uri context)]
    (assoc request-map :url (str host resolved-uri))))

(defn invoke!
  [host api-token endpoint context]
  (let [base-request'    (merge (base-request host) (auth api-token))
        endpoint-request (get endpoints-request endpoint {})
        request-map'     (merge base-request' endpoint-request context)
        request-map      (with-full-url request-map' context)
        response-fn      (or (:response-fn request-map) identity)]
    (-> request-map
        httpkit/req!
        :body
        response-fn)))

(defn create-cluster [token host payload]
  (invoke! host token :clusters/create {:payload payload}))

(defn cluster-list [token host]
  (invoke! host token :clusters/list {}))