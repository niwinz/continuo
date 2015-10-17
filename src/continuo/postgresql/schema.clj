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
            [continuo.impl :as impl]
            [continuo.executor :as exec]
            [continuo.util.template :as tmpl]
            [continuo.util.codecs :as codecs]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Schema Attributes
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- rows->schema
  [results]
  (reduce (fn [acc item]
            (let [opts (codecs/bytes->data (:opts item))
                  ident (:ident opts)]
              (assoc acc ident
                     (assoc opts :ident ident))))
          {}
          results))

(defn refresh-schema-data!
  [tx]
  (let [schema (impl/-get-schema tx)
        conn (impl/-get-connection tx)
        sql "SELECT ident, opts FROM dbschema"
        res (sc/fetch conn sql)]
    (reset! schema (rows->schema res))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Schema Trasnactors
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmulti -apply-schema
  "A polymorphic abstraction for build appropiate
  layout transformation sql for the schema fact."
  (fn [conn [op]] op))

(defmethod -apply-schema :db/add
  [conn [op ident opts]]
  (let [tablename (attrs/normalize-attrname ident "user")
        typename (-> (attrs/lookup-type (:type opts))
                     (attrs/-sql-typename))
        opts (assoc opts :ident ident)
        sql (tmpl/render "bootstrap/postgresql/tmpl-schema-db-add.mustache"
                         {:name tablename :type typename})]
    (sc/execute conn [sql (codecs/data->bytes opts)])))

(defmethod -apply-schema :db/drop
  [conn [op ident]]
  (let [tablename (attrs/normalize-attrname ident "user")
        sql (tmpl/render "bootstrap/postgresql/tmpl-schema-db-drop.mustache"
                         {:name tablename})]
    (sc/execute conn sql)))

(defn -apply-tx
  [conn txid data]
  (let [sql (str "INSERT INTO txlog (id, facts, created_at)"
                 " VALUES (?, ?, current_timestamp)")
        sqlv [sql txid (codecs/data->bytes data)]]
    (sc/execute conn sqlv)))

(defn run-schema
  [tx schema]
  {:pre [(not (empty? schema))]}
  (exec/submit
   #(with-open [conn (impl/-get-connection tx)]
      (sc/atomic conn
        (let [txid (tx/get-next-txid conn)]
          (run! (partial -apply-schema conn) schema)
          (-apply-tx conn txid schema)))
      (refresh-schema-data! tx)
      @(impl/-get-schema tx))))
