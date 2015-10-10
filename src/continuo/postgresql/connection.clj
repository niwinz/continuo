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
            [hikari-cp.core :refer hikari]
            [continuo.executor :as exec]
            [continuo.util :as util]
            [continuo.impl :as impl]))

(def ^{:private true
       :static true}
  +defaults+
  {:connection-timeout 30000
   :idle-timeout 600000
   :max-lifetime 1800000
   :minimum-idle 10
   :maximum-pool-size  10
   :adapter "postgresql"
   :server-name "localhost"
   :port-number 5432})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Types
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(deftype Transactor [datasource schema])
  ;; impl/ITransactorInternal
  ;; (-initialize [it]
  ;;   (bootstrap/initialize it))
  ;; (-create [it]
  ;;   (bootstrap/create it)))

(defn get-connection
  [tx]
  {:pre [(instance? Transactor tx)]}
  (sc/context (.-datasource tx)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Connection Management
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn make-datasource
  [options]
  (let [options (merge +defaults+ options)]
    (hikari/make-datasource options)))

(defn make-connection
  [uri options]
  (let [options (merge options (util/parse-params uri))
        datasource (make-datasource options)
        schema (atom {})]
    (Connnection. connection schema)))

(defmethod impl/connect :postgresql
  [uri options]
  (exec/submit (partial make-connection uri options)))
