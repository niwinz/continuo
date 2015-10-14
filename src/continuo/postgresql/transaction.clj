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

(ns continuo.postgresql.transaction
  (:require [suricatta.core :as sc]
            [taoensso.nippy :as nippy]
            [continuo.postgresql.attributes :as attrs]
            [continuo.impl :as impl]
            [continuo.util.template :as tmpl]
            [continuo.util.codecs :as codecs]
            [continuo.util.uuid :as uuid]
            [continuo.executor :as exec]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Transactions & Identifiers
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-next-txid
  "Get a next transaction identifier."
  [conn]
  (uuid/host-uuid))

(def ^:dynamic *eid-map* nil)

(defn mkeid
  ([]
   (let [val (uuid/host-uuid)]
     (reify
       impl/IEntityId
       (-resolve-eid [_]
         val))))
  ([index]
   (reify
     impl/IEntityId
     (-resolve-eid [_]
       (vswap! *eid-map* (fn [map]
                           (if (get map index)
                             map
                             (assoc map index (uuid/host-uuid)))))
       (get @*eid-map* index)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Locks
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn hold-lock
  ([conn] (hold-lock conn "txlog"))
  ([conn tablename]
   (as-> (format "LOCK TABLE %s IN ACCESS EXCLUSIVE MODE" tablename) sql
     (sc/execute conn sql))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Facts processing
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(comment
  (ct/transact db [[:db/add eid attrname attrvalue]
                   [:db/retract eid attrname attrvalue]]))

(defn- current-attr-value
  "Get the current attribute value in the database. It
  returns `nil` in case of the attr does not has value."
  [conn [_ eid attr val]]
  (let [table (attrs/normalize-attrname attr "user")
        tmpl (str "SELECT * FROM {{table}} "
                  "  WHERE eid=?")
        sql  (tmpl/render-string tmpl {:table table})]
    (sc/fetch-one conn [sql eid])))

(defmulti -apply-fact (fn [_ _ [type]] type))

(defmethod -apply-fact :db/add
  [conn txid [type eid attr val :as fact]]
  (let [table (attrs/normalize-attrname attr "user")
        eid (impl/-resolve-eid eid)
        current-value (current-attr-value conn fact)]
    (if current-value
      (do
        (when (not= current-value val)
          (let [tmpl (str "UPDATE TABLE {{table}} "
                          "  SET modified_at=current_timestamp, "
                          "      content=?, txid=?"
                          "  WHERE eid=?")
                sql (tmpl/render-string tmpl {:table table})]
            (sc/execute conn [sql val txid eid])))
        eid)
      (let [tmpl (str "INSERT INTO {{table}} "
                      "  (eid, txid, modified_at, content)"
                      "  VALUES (?,?,current_timestamp,?)")
            sql (tmpl/render-string tmpl {:table table})]
        (sc/execute conn [sql eid txid val])
        eid))))

(defmethod -apply-fact :db/retract
  [conn txid [type eid attr val]]
  (let [table (attrs/normalize-attrname attr "user")
        tmpl  "DELETE FROM {{table}} WHERE eid = ? AND content = ?"
        sql   (tmpl/render-string tmpl {:table table})
        sqlv  [sql eid val]]
      (sc/execute conn sqlv)))

(defn -apply-tx
  [conn txid facts]
  (let [sql (str "INSERT INTO txtlog (id, part, facts, created_at) "
                 " VALUES (?,'user',?,current_timestamp)")
        data (codecs/data->bytes facts)]
    (sc/execute conn [sql txid data])))

(defn run-transaction
  "Given an connection and seq of operation objects,
  execute them in serie."
  [conn facts]
  (binding [*eid-map* (volatile! {})]
    (let [txid (get-next-txid conn)]
      (-apply-tx conn txid facts)
      (let [ids (reduce #(conj %1 (-apply-fact conn txid %)) facts)]
        (filterv identity ids)))))

(defn transact
  [tx facts]
  (exec/submit
   #(let [conn (impl/-get-connection tx)]
      (sc/atomic conn
        (hold-lock conn)
        (run-transaction conn facts)))))
