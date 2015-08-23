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

(ns continuo.postgresql.bootstrap
  (:require [suricatta.core :as sc]
            [hikari-cp.core :refer hikari]
            [continuo.executor :as exec]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Database Creation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- table-installed?
  [ctx tablename]
  (let [sql (format "SELECT to_regclass('public.%s')" tablename)
        rst (sc/fetch-one ctx sql)]
    (boolean (first rst))))

(defn- installed?
  "Check if the main database layour is already installed."
  [ctx]
  (every (partial check ctx) ["txlog", "properties", "schemaview", "entity"]))

(defn create'
  [ctx]
  (when-not (installed? ctx)
    (let [bsql (slurp (io/resource "persistence/postgresql/bootstrap.sql"))]
      (sc/execute ctx bsql))))

(defn create
  [conn]
  (let [ctx (:context conn)]
    (exec/submit #(sc/atomic-apply ctx create'))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Connection Management
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

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

(defn connect'
  [options]
  (let [options (merge +defaults+ options)]
    (sc/context (hikari/make-datasource options))))

(defn connect
  [options]
  (exec/submit #(connect' options)))
