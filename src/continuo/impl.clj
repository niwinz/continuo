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

(ns continuo.impl
  "Internal api."
  (:require [promissum.core :as p]
            [cats.core :as m]
            [continuo.util.uuid :as uuid])
  (:import java.net.URI
           java.util.UUID))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Protocols
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; NOTE: all protocol functions should return futures.

(defprotocol ITransactorInternal
  (-initialize [_] "Execute initial operations.")
  (-create [_] "Create the databaase.")
  (-get-connection [_] "Get the database connection."))

(defprotocol IEntityId
  (-resolve-eid [_] "Get proper uuid."))

(extend-type UUID
  IEntityId
  (-resolve-eid [this] this))

(defprotocol ITransactor
  (-transact [_ fact] "Register a transaction.")
  (-entity [_ eid] "Get a entity by eid."))

;; (defprotocol ITransactor
;;   (-transact [_ data] "Register a transaction.")
;;   (-query [_ spec] "Query the database.")
;;   (-pull [_ spec entity] "Pull a entity from the database."))

(defprotocol ISchemaTransactor
  (-get-schema [_] "Get the schema reference.")
  (-run-schema [_ s] "Execute the schema manipulation commands."))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Entity ID mapper
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^:dynamic *eid-map* nil)

(defn mkeid
  ([]
   (let [val (uuid/host-uuid)]
     (reify
       IEntityId
       (-resolve-eid [_]
         val))))
  ([index]
   (reify
     IEntityId
     (-resolve-eid [_]
       (vswap! *eid-map* (fn [map]
                           (if (get map index)
                             map
                             (assoc map index (uuid/host-uuid)))))
       (get @*eid-map* index)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Internal Api
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmulti connect
  (fn [^URI uri options]
    (keyword (.getScheme uri))))

(defn open
  [^URI uri options]
  (m/mlet [conn (connect uri options)]
    (-initialize conn)))

(defn create
  [^URI uri options]
  (m/mlet [conn (connect uri options)]
    (-create conn)))
