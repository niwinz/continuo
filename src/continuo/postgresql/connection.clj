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

(ns continuo.postgresql.connection
  "A connection management."
  (:require [suricatta.core :as sc]
            [hikari-cp.core :as hikari]
            [continuo.executor :as exec]
            [continuo.util.uri :as uri]
            [continuo.impl :as impl]
            [continuo.postgresql.bootstrap :as boot]
            [continuo.postgresql.transaction :as tx]
            [continuo.postgresql.schema :as schema]))

(def ^{:private true
       :doc "The connection to use in test transactor"}
  +test-connection+ nil)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Types
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(deftype Transactor [datasource schema]
  impl/ITransactorInternal
  (-initialize [it] (boot/initialize it))
  (-create [it] (boot/create it))
  (-get-connection [_] (sc/context datasource
                           {:isolation-level :serializable}))
  impl/ITransactor
  (-transact [it facts]
    (tx/transact it facts))
  (-entity [it eid]
    (tx/entity it eid))

  impl/ISchemaTransactor
  (-get-schema [_] schema)
  (-run-schema [it s] (schema/run-schema it s)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Connection Management
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^{:private true :static true}
  +defaults+
  {:connection-timeout 30000
   :idle-timeout 600000
   :max-lifetime 1800000
   :minimum-idle 0
   :maximum-pool-size 10
   :adapter "postgresql"
   :server-name "localhost"
   :port-number 5432})

(defn make-datasource
  [options]
  (letfn [(mergefn [resultv newv]
            (if (empty newv)
              resultv
              newv))]
    (let [options (merge-with mergefn +defaults+ options)]
      (hikari/make-datasource options))))

(defn parse-uri
  [uri]
  (let [uridata (uri/parse uri)]
    (merge
     {:database-name (:path uridata)
      :port-number (:port uridata)
      :server-name (:host uridata)
      :username (:username uridata)
      :password (:password uridata)}
     (:params uridata))))

(defn make-connection
  [uri options]
  (let [options (merge (parse-uri uri) options)
        datasource (make-datasource options)
        schema (atom nil)]
    (Transactor. datasource schema)))

(defmethod impl/connect :postgresql
  [uri options]
  (exec/submit (partial make-connection uri options)))
