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
            [continuo.util.template :as tmpl]
            [continuo.util.codecs :as codecs]))

(defn normalize-attrname
  [attrname partition]
  {:pre [(keyword? attrname) (string? partition)]}
  (let [ns (namespace attrname)
        nm (name attrname)]
    (str/lower (str partition "_" ns "__" nm))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Attribute SQL Generator
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defprotocol IType
  (-sql-typename [_]))

(deftype TString []
  IType
  (-sql-typename [_] "text"))

(defn lookup-type
  [typename]
  {:pre [(keyword? typename)]}
  (case typename
    :string (TString.)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Schema Attributes
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- prepare-schema
  [results]
  (reduce (fn [acc item]
            (let [opts (codecs/bytes->data (:opts item))
                  ident (:ident item)]
              (assoc acc ident
                     (assoc opts :ident ident))))
          {}
          results))

(defn populate-chema
  [conn schema]
  (let [sql "SELECT ident, opts FROM dbschema"
        res (sc/fetch conn sql)]
    (reset! schema (prepare-schema res))))

