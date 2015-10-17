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
  (:require [clojure.edn :as edn]
            [cuerdas.core :as str]
            [suricatta.core :as sc]
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Locks
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn hold-lock
  ([conn] (hold-lock conn "txlog"))
  ([conn tablename]
   (as-> (format "LOCK TABLE %s IN ACCESS EXCLUSIVE MODE" tablename) sql
     (sc/execute conn sql))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Transactions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- current-attr-value
  "Get the current attribute value in the database. It
  returns `nil` in case of the attr does not has value."
  [conn [_ eid attr val]]
  (let [table (attrs/normalize-attrname attr "user")
        tmpl (str "SELECT * FROM {{table}} "
                  "  WHERE eid=?")
        eid (impl/-resolve-eid eid)
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
          (let [tmpl (str "UPDATE {{table}} "
                          "  SET modified_at=current_timestamp, "
                          "      content=?, txid=?"
                          "  WHERE eid=?")
                sql (tmpl/render-string tmpl {:table table})]
            (sc/execute conn [sql val txid eid])))
        eid)
      (let [tmpl (str "INSERT INTO {{table}} "
                      "  (eid, txid, modified_at, content)"
                      "  VALUES (?,?,current_timestamp,?)")
            sql1 (tmpl/render-string tmpl {:table table})
            sql2 "INSERT INTO entity_attrs (eid, attr) VALUES (?, ?)"]
        (sc/execute conn [sql1 eid txid val])
        (sc/execute conn [sql2 eid (pr-str attr)])
        eid))))

(defmethod -apply-fact :db/retract
  [conn txid [type eid attr val]]
  (let [table (attrs/normalize-attrname attr "user")
        tmpl  "DELETE FROM {{table}} WHERE eid = ? AND content = ?"
        sql   (tmpl/render-string tmpl {:table table})
        eid (impl/-resolve-eid eid)
        sqlv  [sql eid val]]
    (when (pos? (sc/execute conn sqlv))
      (let [sql (str "DELETE FROM entity_attrs"
                     " WHERE eid=? AND attr=?")]
        (sc/execute conn [sql eid (pr-str attr)])))
    nil))

(defn -apply-tx
  [conn txid facts]
  (let [sql (str "INSERT INTO txlog (id, part, facts, created_at) "
                 " VALUES (?,'user',?,current_timestamp)")
        facts (mapv #(update % 1 impl/-resolve-eid) facts)
        data (codecs/data->bytes facts)]
    (sc/execute conn [sql txid data])))

(defn run-transaction
  "Given an connection and seq of operation objects,
  execute them in serie."
  [conn facts]
  (binding [impl/*eid-map* (volatile! {})]
    (let [txid (get-next-txid conn)]
      (-apply-tx conn txid facts)
      (let [ids (mapv #(-apply-fact conn txid %) facts)]
        (set (filterv identity ids))))))

(defn transact
  [tx facts]
  (exec/submit
   #(let [conn (impl/-get-connection tx)]
      (sc/atomic conn
        (hold-lock conn)
        (run-transaction conn facts)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Entity Retrieval
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- get-attributes
  [conn eid]
  (let [sql (str "SELECT attr FROM entity_attrs"
                 " WHERE eid = ?")]
    (mapv (fn [record]
            (edn/read-string (:attr record)))
          (sc/fetch conn [sql eid]))))

(defn- make-query-sql
  [attr]
  (let [table (attrs/normalize-attrname attr "user")
        tmpl  (str "(SELECT eid, content FROM {{table}}"
                   " WHERE eid=?)")]
    (tmpl/render-string tmpl {:table table})))


"SELECT t1.eid, t1.content, t2.content, t3.content FROM foo_foo AS t1
  INNER JOIN foo_bar AS t2 ON (t1.eid = t2.eid)
  INNER JOIN foo_baz AS t3 ON (t1.eid = t3.eid)
  WHERE eid = ?"


(defn columns-sql
  [attrs]
  (reduce (fn [acc [index attr]]
            (let [field (str "t" index ".content AS f" index)]
              (str acc ", " field)))
          "t0.eid"
          (map-indexed vector attrs)))

(defn join-sql
  [attrs]
  (let [attrs (map-indexed vector attrs)
        [index attr] (first attrs)
        table (attrs/normalize-attrname attr "user")
        alias (str "t" index)
        tmpl (str "{{table}} AS {{alias}}")
        initial (tmpl/render-string tmpl {:table table :alias alias})]
    (reduce (fn [acc [index attr]]
              (let [table (attrs/normalize-attrname attr "user")
                    alias (str "t" index)
                    prevalias (str "t" (dec index))
                    tmpl (str " INNER JOIN {{table}} AS {{alias}}"
                              " ON ({{prevalias}}.eid = {{alias}}.eid)")]
                (str acc (tmpl/render-string tmpl {:table table
                                                   :alias alias
                                                   :prevalias prevalias}))))
            initial (rest attrs))))

(defn get-entity
  [conn eid]
  (let [eid (impl/-resolve-eid eid)
        attrs (get-attributes conn eid)
        columns-sql (columns-sql attrs)
        join-sql (join-sql attrs)
        sqltmpl "SELECT {{columns}} FROM {{join}} WHERE t0.eid=?"
        sql (tmpl/render-string sqltmpl {:columns columns-sql
                                         :join join-sql})
        result (sc/fetch-one conn [sql eid] {:format :row})]
    (loop [attrs attrs
           values (rest result)
           result {:eid eid}]
      (let [attr (first attrs)
            value (first values)]
        (if attr
          (recur (rest attrs)
                 (rest values)
                 (assoc result attr value))
          result)))))

(defn entity
  [tx eid]
  (exec/submit
   #(let [conn (impl/-get-connection tx)]
      (get-entity conn eid))))
