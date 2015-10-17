(ns continuo.util.uri
  (:require [clojure.walk :refer [stringify-keys keywordize-keys]]
            [clojure.edn :as edn]
            [cuerdas.core :as str])
  (:import java.net.URLDecoder
           java.net.URI))

(defn- coerce-value
  [value]
  (let [result (edn/read-string value)]
    (if (symbol? result)
      (str result)
      result)))

(defn querystring->map
  [^String querystring]
  (persistent!
   (reduce (fn [acc item]
             (let [[key value] (str/split item "=")
                   [key value] [(URLDecoder/decode key)
                                (URLDecoder/decode value)]]
               (assoc! acc
                       (keyword (str/dasherize key))
                       (coerce-value value))))
           (transient {})
           (str/split querystring "&"))))

(defn parse
  [^URI uri]
  (let [userinfo (.getUserInfo uri)
        host (.getHost uri)
        port (.getPort uri)
        path (.getPath uri)]
    (merge
     {:host host
      :path (str/ltrim path "/")
      :params (querystring->map (.getQuery uri))}
     (when-not (empty? userinfo)
       (let [[user password] (str/split userinfo ":")]
         {:username user
          :password password}))
     (when (pos? port)
       {:port port}))))

(defprotocol IURIFactory
  (-coerce [_] "Cast type to valid uri."))

(extend-protocol IURIFactory
  java.lang.String
  (-coerce [s] (URI/create s))

  URI
  (-coerce [u] u))

