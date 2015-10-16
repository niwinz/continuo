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

(ns continuo.core
  "Event source based persistence."
  (:require [continuo.impl :as impl]
            [continuo.util :as util]))

(defn open
  "Given an uri and optionally a options hash-map,
  create a transactor.

  The backend used for the connection is resolved
  using the uri scheme and backend options are parsed
  from the query params.

  This function accepts additionally a options map
  thar serves for configure serializer, compression
  and other similar things that are not related
  to the connection parameters."
  ([uri]
   (impl/open (util/->uri uri) {}))
  ([uri options]
   (impl/open (util/->uri uri) options)))

(defn create
  "Given an uri, initializes the initial database
  layout on specified uri.

  This function return a Future that can be resolved
  successfully with `nil` or rejected with an error.

  If you call repeatedly this function, the second call
  wiil be a rejected future with an error indicating you
  that the database is already exists."
  [uri]
  (impl/create (util/->uri uri) {}))

(defn run-schema
  "Given an connection, run schema operations in
  one transaction."
  [conn schema]
  (impl/-run-schema conn schema))

(defn get-schema
  "Get the current schema."
  [conn]
  (let [schema (impl/-get-schema conn)]
    (set (vals @schema))))

(defn transact
  [conn facts]
  (impl/-transact conn facts))

(defn mkeid
  ([] (impl/mkeid))
  ([index] (impl/mkeid index)))


(comment
  (def uri "pgsql://localhost:5432/foobar?dbname=test")

  (ct/create uri)
  ;; => #<cats.monad.either.Right [nil]>

  (def conn (ct/open uri))
  ;; => #<continuo.database.Database@4958295d4>
  )

;; The `schema` function should be used for execute schema manipulation
;; commands, defined as data-driven DSL.
;; The DSL consists in something similar to this:

(comment
  (ct/run-schema! db [[:add :user/username {:unique true :type :text}]
                      [:alter :user/age {:type :short}]
                      [:rename :user/name :user/fullname]]))

;; The `transact` function should be used to persist a collection
;; of facts.

(comment
  (ct/transact db [[:db/add eid attrname attrvalue]
                   [:db/retract eid attrname attrvalue]]))
