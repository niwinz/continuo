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

(ns continuo.postgresql.schema
  (:require [suricatta.core :as sc]
            [continuo.util.template :as tmpl]
            [continuo.executor :as exec]))

;; TODO: add props validation

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Attribute SQL Generator
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmulti generate-create-sql
  "Generate a valid DDL operation for create the attribute
  table depending on the attribute type."
  :type)

(defmethod generate-create-sql :continuo/string
  [{:keys [attr] :as opts}]
  (let [sql (str "CREATE TABLE {{ attr }} ("
                 "  eid uuid PRIMARY KEY,"
                 "  txid bigint,"
                 "  created_at timestamptz default now(),"
                 "  modified_at timestamptz,"
                 "  content text"
                 ") WITH (OIDS=FALSE);")]
    (tmpl/render-string sql {:attr attr})))

(defmethod generate-create-sql :default
  [{:keys [attr] :as opts}]
  (throw (ex-info "The type is not implemented or not specified." {})))

(defn- generate-drop-sql
  "Generate a valid DDL operation for create the attribute
  table depending on the attribute type."
  [attr]
  (let [sql (str "DROP TABLE {{ attr }};")]
    (tmpl/render-string sql {:attr attr})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Schema Compiler
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(declare normalize-attrname)

(defmulti compile-op
  "Compile a chema operation into a ready
  to execute operation."
  (fn [fact] (first fact)))

(defmethod compile-op :db/add
  [[_ attrname props]]
  (let [attr (normalize-attrname attrname)
        sql (generate-create-sql (assoc props :attr attr))]
    (fn [execute]
      (execute sql))))

(defmethod compile-op :db/drop
  [[_ attrname]]
  (let [attr (normalize-attrname attrname)
        sql (generate-drop-sql attr)]
    (fn [execute]
      (execute sql))))

(defmethod compile-op :default
  [[op]]
  (throw (ex-info (format "The schema operation %s is not supported." op) {})))

(defn compile-schema
  "Compile a schema entries."
  [schema]
  (reduce (fn [acc v]
            (fn [execute]
              (let [callable (compile-op v)]
                (acc execute)
                (callable execute))))
          identity
          schema))

;; (defn execute-schema
;;   [schema ]
;;   {:pre [(seq schema)]}
;;   (csch (fn [sql]
;;             (println "EXECUTING:" sql)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Utils
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- normalize-attrname
  [attrname]
  {:pre [(keyword? attrname)]}
  (let [nspart (namespace attrname)
        nmpart (name attrname)]
    (str nspart "_" nmpart)))



