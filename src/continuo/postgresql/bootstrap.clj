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
  (:require [clojure.java.io :as io]
            [promissum.core :as p]
            [suricatta.core :as sc]
            [continuo.util.exceptions :refer [unwrap-exception]]
            [continuo.executor :as exec]
            [continuo.impl :as impl]
            [continuo.postgresql.bootstrap :as boot]
            [continuo.postgresql.schema :as schema]
            [continuo.postgresql.attributes :as attrs]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Database Creation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- table-installed?
  [conn tablename]
  (let [sql (format "SELECT to_regclass('public.%s') as regclass" tablename)
        rst (sc/fetch-one conn sql)]
    (boolean (:regclass rst))))

(defn- installed?
  "Check if the main database layour is already installed."
  [conn]
  (every? (partial table-installed? conn)
          ["txlog", "dbschema"]))

(def ^:static
  bootstrap-sql-file "bootstrap/postgresql/bootstrap.sql")

(defn create'
  [conn]
  (if (installed? conn)
    false
    (let [sql (slurp (io/resource bootstrap-sql-file))]
      (sc/execute conn sql)
      true)))

(defn create
  [tx]
  (exec/submit
   #(let [conn (impl/-get-connection tx)]
      (sc/atomic-apply conn create'))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Database Initialization
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn initialize
  [tx]
  (let [conn (impl/-get-connection tx)
        schema (impl/-get-schema tx)]
    (letfn [(on-error [e]
              (if (instance? org.postgresql.util.PSQLException e)
                (throw (ex-info "Database not initialized."
                                {:type :error/db-not-initialized}))
                (throw e)))
            (do-initialize []
              (schema/refresh-schema-data! tx)
              tx)]
      (-> (exec/submit do-initialize)
          (p/catch on-error)))))
