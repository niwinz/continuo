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
            [cuerdas.core :as str]
            [continuo.postgresql.transaction :as tx]
            [continuo.postgresql.attributes :as attrs]
            [continuo.postgresql.connection :as conn]
            [continuo.util.template :as tmpl]
            [continuo.util.codecs :as codecs]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Schema Trasnactors
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmulti -apply-schema
  "A polymorphic abstraction for build appropiate
  layout transformation sql for the schema fact."
  (fn [conn [op]] op))

(defmethod -apply-schema :db/add
  [conn [op ident opts]]
  (let [tablename (attrs/normalize-attrname ident "attrs")
        typename (-> (attrs/lookup-type (:type opts))
                     (attrs/-sql-typename))
        sql (tmpl/render "postgresql/tmpl-schema-db-add.mustache"
                         {:name tablename :type typename})]
    (sc/execute conn [sql (codecs/data->bytes opts)])))

(defmethod -apply-schema :db/drop
  [conn [op ident]]
  (let [tablename (attrs/normalize-attrname ident "attrs")
        sql (tmpl/render "postgresql/tmpl-schema-db-drop.mustache"
                         {:name tablename})]
    (sc/execute conn sql)))

(defn -apply-tx
  [conn txid data]
  (let [sql ["INSERT INTO txlog (id, part, facts) VALUES (?,?,?)"
             txid "schema" data]]
    (sc/execute conn sql)))

(defn run-schema
  [context schema]
  (let [conn (.-connection context)]
    (ct/atomic conn
      (let [txid (tx/get-next-txid conn)]
        (run! #(-apply-schema conn %) schema)
        (-apply-tx conn txid schema)))))

;; (comment
;;   (ct/run-schema! db [[:db/add :user/username {:unique true :type :text}]
;;                       [:db/drop :user/name]]))

