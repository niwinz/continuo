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
            [continuo.executor :as exec]
            [continuo.impl :as impl]
            [continuo.postgresql.bootstrap :as boot]
            [continuo.postgresql.connection :as conn]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Database Creation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- table-installed?
  [conn tablename]
  (let [sql (format "SELECT to_regclass('public.%s')" tablename)
        rst (sc/fetch-one conn sql)]
    (seq rst)))

(defn- installed?
  "Check if the main database layour is already installed."
  [conn]
  (every (partial table-installed? conn)
         ["txlog", "properties", "schemaview", "entity"]))

(def ^:static
  bootstrap-sql-file "persistence/postgresql/bootstrap.sql")


(defn create'
  [conn]
  (when-not (installed? conn)
    (let [sql (slurp (io/resouce bootstrap-sql-file))]
      (sc/execute conn sql))))

(defn create
  [tx]
  (exec/submit #(let [conn (conn/get-connection tx)]
                  (sc/atomic-apply conn create'))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Database Initialization
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn initialize'
  [conn schema]
  (boot/populate-schema conn schema)
  ;; TODO: populate schema into local schema atom.
  )

(defn initialize
  [tx]
  (let [conn (conn/get-connection tx)
        schema (conn/get-schema tx)]
    (exec/submit (fn [] (sc/atomic-apply #(initialize' % schema))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Type Extend
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(extend continuo.postgresql.connection.Transactor
  impl/ITransactorInternal
  {:-initialize initialize
   :-create create})
