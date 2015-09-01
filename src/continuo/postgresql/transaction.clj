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
            [continuo.util.template :as tmpl]
            [continuo.util.codecs :as codecs]
            [continuo.executor :as exec]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Transaction Identifier
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-last-txid
  "Get the last tx identifier it if exists or return nil."
  [conn]
  (let [sql "SELECT id FROM txlog ORDER BY created_at DESC LIMIT 1"]
    (first (sc/fetch-one conn sql))))

(defn get-next-txid
  "Get a next transaction identifier."
  [conn]
  (let [lastid (get-last-txid conn)]
    (if (nil? lastid) 1 (inc lastid))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Facts processing
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defprotocol IOperation
  (-execute [_ conn txid] "Execute the operation."))

(deftype AddOperation [partition attr value]
  IOperation
  (-execute [_ conn txid]
    (let [table (attrs/-normalized-name attr partition)
          tmpl  (str "INSERT INTO {{table}} "
                     "  (eid, txid, modified_at, content)"
                     "  VALUES (?,?,now(),?)")
          sql (tmpl/render-string tmpl {:table table})
          sqlv  [sql eid txid value]]
      (sc/execute conn sqlv))))

(deftype RetractOperation [partition attr value]
  IOperation
  (-execute [_ conn txid]
    (let [table (attrs/-normalized-name attr partition)
          tmpl  (str "UPDATE {{table}} SET content = ? modified_at = now() "
                     " WHERE eid = ?")
          sql   (tmpl/render-string tmpl {:table table})
          sqlv  [sql value eid]]
      (sc/execute conn sqlv))))

;; (deftype DropOperation [partition attr value]
;;   IOperation
;;   (-execute [_ conn txid]
;;     (let [table (attrs/-normalized-name attr partition)
;;           tmpl  "DELTE FROM {{table}} WHERE eid = ?"
;;           sql   (tmpl/render-string tmpl {:table table})
;;           sqlv  [sql eid]]
;;       (sc/execute conn sqlv))))

(deftype TxOperation [partition data]
  IOperation
  (-execute [_ conn txid]
    (let [sql ["INSERT INTO txlog (id, part, facts) VALUES (?,?,?)"
               txid partition data]]
      (sc/execute conn sql))))

(defn compile-fact
  "Given a destructured fact, returns a ready to execute
  operation instance.
  The operation consists on correctly insert the data in
  the appropiate default materialized view."
  [context partition [op eid attrname value]]
  ;; resolve-attr :: context -> string -> Attribute
  (let [attr (attrs/resolve-attr context attrname)]
    (case op
      :db/add (AddOperation. partition eid attr value)
      :db/retract (RetractOperation. partition eid attr value))))
      ;; :db/drop (DropOperation. partition eid attr value))))

(defn compile-txop
  [context [partition facts]]
  (TxOperation. partition (codecs/data->bytes facts)))

(defn compile-ops
  [context partition facts]
  (let [txop (compile-txop context [partition facts])
        inops (mapv (partial compile-fact context partition) facts)]
    (into [txop] inops)))

(defn run-tx
  "Given an connection and seq of operation objects,
  execute them in serie."
  [conn operations]
  (let [txid (get-next-txid conn)]
    (run! #(-execute % conn txid) operations))

(defn run-in-tx
  [conn func]
  (sc/atomic conn
    ;; TODO: exclusive share lock
    (func conn)))

(defn transact*
  [context partition facts]
  (let [ops (compile-ops context partition facts)
        conn (.-connection context)]
    (run-in-tx conn #(run-tx % ops))))

(defn transact
  [context facts]
  (transact* context "user" facts))
