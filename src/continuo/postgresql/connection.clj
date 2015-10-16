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
            [continuo.util :as util]
            [continuo.impl :as impl]
            [continuo.postgresql.bootstrap :as boot]
            [continuo.postgresql.transaction :as tx]
            [continuo.postgresql.schema :as schema]))

(def ^{:private true :static true}
  +defaults+
  {:connection-timeout 30000
   :idle-timeout 600000
   :max-lifetime 1800000
   :minimum-idle 10
   :maximum-pool-size  10
   :adapter "postgresql"
   :server-name "localhost"
   :port-number 5432})

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
  (-get-connection [_] (sc/context datasource))

  impl/ITransactor
  (-transact [it facts]
    (tx/transact it facts))
  (-entity [it eid]
    (tx/entity it eid))

  impl/ISchemaTransactor
  (-get-schema [_] schema)
  (-run-schema [it s] (schema/run-schema it s)))

(deftype TestTransactor [connecton schema]
  impl/ITransactorInternal
  (-initialize [it] (boot/initialize it))
  (-create [it] (boot/create it))
  (-get-connection [_] connecton)

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

(defn make-datasource
  [options]
  (let [options (merge +defaults+ options)]
    (hikari/make-datasource options)))

(defn make-connection
  [uri options]
  (let [options (merge options (util/parse-params uri))
        datasource (make-datasource options)
        schema (atom nil)]
    (Transactor. datasource schema)))

(defmethod impl/connect :postgresql
  [uri options]
  (exec/submit (partial make-connection uri options)))

(defmethod impl/connect :postgresql-test
  [uri options]
  (let [conn +test-connection+]
    (assert conn "Connection should exists")
    (exec/submit
     #(TestTransactor. conn (atom nil)))))
