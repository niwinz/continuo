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
            [cuerdas.core :as str]
            [continuo.postgresql.attributes :as attrs]
            [continuo.postgresql.types :as types]))

(comment
  (ct/run-schema! db [[:db/add :user/username {:unique true :type :text}]
                      [:db/drop :user/name]]))

(defmulti -execute-schema
  "A polymorphic abstraction for build appropiate
  layout transformation sql for the schema fact."
  (fn [conn type & params] type))

(defmethod -execute-schema :db/add
  [conn _ ident opts]
  (let [tablename (attrs/normalize-attrname ident "attrs")
        typename (-> (types/lookup-type (:type opts))
                     (types/-sql-typename))
        sql (tmpl/render "postgresql/tmpl-schema-db-add.mustache"
                         {:name tablename :type typename})]
    (sc/execute conn [sql (codecs/data->bytes opts)])))

(defmethod -execute-schema :db/drop
  [conn _ ident]
  (let [tablename (attrs/normalize-attrname ident "attrs")
        sql (tmpl/render "postgresql/tmpl-schema-db-drop.mustache"
                         {:name tablename})]
    (sc/execute conn sql)))

(defn -execute-transact
  [conn txid data]
  (let [sql ["INSERT INTO txlog (id, part, facts) VALUES (?,?,?)"
             txid "schema" data]]
    (sc/execute conn sql)))

(defn run-schema
  [context schema]
  (let [ops (map (partial compile-op context) schema)
        conn (.-connection context)]
    (ct/atomic conn
      (let [txid (tx/get-next-txid conn)]
        (run! #(apply -execute-schema conn %) schema)
        (-execute-transact conn txid schema))


      (run! (fn [op] (-execute op conn)) ops))))
