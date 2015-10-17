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

(ns continuo.util.codecs
  (:require [taoensso.nippy :as nippy]))

(defn data->bytes
  "Serialize a clojure data into a binary format."
  [data]
  (nippy/freeze data))

(defn bytes->data
  "Deserialize bytes into clojure data structures."
  [input]
  (nippy/thaw input))

(defn str->bytes
  "Convert string to java bytes array"
  ([^String s]
   (str->bytes s "UTF-8"))
  ([^String s, ^String encoding]
   (.getBytes s encoding)))

(defn bytes->str
  "Convert octets to String."
  ([^bytes data]
   (bytes->str data "UTF-8"))
  ([^bytes data, ^String encoding]
   (String. data encoding)))
