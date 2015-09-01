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

(defprotocol IAttribute
  (-normalized-name [_ partition] "Get normalized attribute name."))

(deftype Attribute [name type opts]
  clojure.lang.Named
  (getNamespace [_] (namespace name))
  (getName [_] (clojure.core/name name))

  IAttribute
  (-normalized-name [_ partition]
    (let [ns (namespace attr)
          nm (name attr)]
      (str/lower (str partition "_" ns "__" nm)))))

(defn attribute?
  [attr]
  (instance? Attribute attr))

(defn schema-attribute?
  [attr]
  (and (attribute? attr)
       (= (namespace attr) "db")))

(defn attribute
  ([name type]
   (attribute name type {}))
  ([name type opts]
   (Attribute. name type opts)))

(def ^:private
  +builtin-attributes+
  {:db/ident (attribute :db/ident :string {:unique true})
   :db/unique (attribute :db/unique :boolean {})})

(defn resolve-attr
  "Given a context and the attribute name
  return the attribute definition."
  [context attrname]
  {:pre [(keyword? attrname)]}
  (if (schema-attribute? attrname)
    (get +builtin-attributes+ attrname)
    (let [schema (.-schema context)]
      ;; TODO: do stuff
      )))

;; (defn- get-create-table-sql
;;   [attr]
;;   {:pre [(attr? attr)]}
;;   (let [tablename (get-table-name attr)
;;         typename (types/-sql-typename (.-type attr))
;;         template (str "CREATE TABLE {{ name }} ("
;;                       "  eid uuid PRIMARY KEY,"
;;                       "  txid bigint,"
;;                       "  created_at timestamptz default now(),"
;;                       "  modified_at timestamptz,"
;;                       "  content {{ type }}"
;;                       ") WITH (OIDS=FALSE);")]
;;     (tmpl/render-string template {:name tablename
;;                                   :type typename})))

;; (defn generate-drop-sql
;;   "Generate a valid DDL operation for create the attribute
;;   table depending on the attribute type."
;;   [attr]
;;   (let [sql (str "DROP TABLE {{ attr }};")]
;;     (tmpl/render-string sql {:attr attr})))

