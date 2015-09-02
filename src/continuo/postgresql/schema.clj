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

(ns continuo.postgresql.attributes
  (:require [suricatta.core :as sc]
            [cuerdas.core :as str]))

(comment
  (ct/run-schema! db [[:add :user/username {:unique true :type :text}]
                      [:alter :user/age {:type :short}]
                      [:rename :user/name :user/fullname]]))


(defprotocol ISchemaOperation
  (-execute [_ conn txid] "Execute the operation."))

(deftype AddOperation [ident opts]
  ISchemaOperation
  (-execute [_ conn]
    (let [tablename (attrs/normalize-attrname ident "attrs")
          typename (-> (types/lookup-type (:type opts))
                       (types/-sql-typename))
          template (str "CREATE TABLE {{ name }} ("
                        "  eid uuid PRIMARY KEY,"
                        "  txid bigint,"
                        "  created_at timestamptz default now(),"
                        "  modified_at timestamptz,"
                        "  content {{ type }}"
                        ") WITH (OIDS=FALSE);")]
      (tmpl/render-string template {:name tablename
                                    :type typename})))
  (-rollback [_ conn]
    (let [tablename (attrs/normalize-attrname ident "attrs")
          sql (str "DROP TABLE IF EXISTS {{ name }};")]
      (tmpl/render-string sql {:name tablename}))))

(defn compile-op
  [context [op ident opts]]
  (case op
    :db/add (AddOperation. ident opts)
    :db/drop (DropOperation. ident opts)))

(defn run-schema
  [context schema]
  (let [ops (compile-sch
