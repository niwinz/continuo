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


(deftype Attribute [name type opts]
  clojure.lang.Named
  (getNamespace [_] (namespace name))
  (getName [_] (clojure.core/name name)))

(defn attribute?
  [attr]
  (instance? Attribute attr))

(defn attribute
  [name type opts]
  (with-meta (Attribute. name type opts)
    {::type ::standard}))

;; (defprotocol IAttribute
;;   (-create-table-sql [_] "Generate a sql for create table ddl.")
;;   (-table-name [_] "Generate a normalized table name for attribute."))

(declare attribute?)

(defn- get-table-name
  [attr]
  {:pre [(attribute?? attr)]}
  (let [ns (namespace attr)
        nm (name attr)]
    (str prefix "_" ns "__" nm)))

(defn- get-create-table-sql
  [attr]
  {:pre [(attr? attr)]}
  (let [tablename (get-table-name attr)
        typename (types/-sql-typename (.-type attr))
        template (str "CREATE TABLE {{ name }} ("
                      "  eid uuid PRIMARY KEY,"
                      "  txid bigint,"
                      "  created_at timestamptz default now(),"
                      "  modified_at timestamptz,"
                      "  content {{ type }}"
                      ") WITH (OIDS=FALSE);")]
    (tmpl/render-string template {:name tablename
                                  :type typename})))
(defn special-attrname?
  [attrname]
  {:pre [(keyword? attrname)]}
  (= (namespace attrname) "db"))

(def ^:private
  +builtin-attributes+
  {:db/ident (Attribute. "db" :db/ident :continuo/string {:unique true})})

(defn resolve-attr
  "Given a context and the attribute name
  return the attribute definition."
  [context attrname]
  {:pre [(keyword? attrname)]}
  (let [schema (.-schema context)]
    ;; TODO: do stuff
    ))

(defn generate-drop-sql
  "Generate a valid DDL operation for create the attribute
  table depending on the attribute type."
  [attr]
  (let [sql (str "DROP TABLE {{ attr }};")]
    (tmpl/render-string sql {:attr attr})))

