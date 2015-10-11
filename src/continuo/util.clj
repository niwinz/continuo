;; Copyright 2015 Andrey Antukh <niwi@niwi.nz>
;;
;; Licensed under the Apache License, Version 2.0 (the "License")
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;;
;;     http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.

(ns continuo.util
  (:require [clojure.walk :refer [stringify-keys keywordize-keys]]
            [cuerdas.core :as str])
  (:import java.net.URLDecoder
           java.net.URI))

(defn querystring->map
  [^String querystring]
  (persistent!
   (reduce (fn [acc item]
             (let [[key value] (str/split item "=")
                   [key value] [(URLDecoder/decode key)
                                (URLDecoder/decode value)]]
               (assoc! acc key value)))
           (transient {})
           (str/split querystring "&"))))

(defn parse-params
  [^URI uri]
  (let [userinfo (.getUserInfo uri)
        host (.getHost uri)
        port (.getPort uri)]
    (merge
     (-> (.getQuery uri)
         (querystring->map)
         (keywordize-keys))
     {:host host}
     (when-not (empty? userinfo)
       (let [[user password] (str/split userinfo ":")]
         {:username user
          :password password}))
     (when port
       {:port port}))))

(defn str->bytes
  "Convert string to java bytes array"
  ([^String s]
   (str->bytes s "UTF-8"))
  ([^String s, ^String encoding]
   (.getBytes s encoding)))

(defn bytes->str
  "Convert octets to String."
  ([^bytes data]
   (bytes->str data "UTF-8"))
  ([^bytes data, ^String encoding]
   (String. data encoding)))

(defprotocol IURIFactory
  (->uri [_] "Cast type to valid uri."))

(extend-protocol IURIFactory
  java.lang.String
  (->uri [s]
    (URI/create s))

  URI
  (->uri [u]
    u))

