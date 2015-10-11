
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

(ns continuo.util.template
  "A lightweight abstraction over mustache.java
  template engine."
  (:require [clojure.walk :as walk]
            [clojure.java.io :as io])
  (:import java.io.StringReader
           java.io.StringWriter
           java.util.HashMap
           com.github.mustachejava.DefaultMustacheFactory
           com.github.mustachejava.Mustache))

;; DOCUMENTATION: http://mustache.github.io/mustache.5.html

(def ^:private
  ^DefaultMustacheFactory
  +mustache-factory+ (DefaultMustacheFactory.))

(defn- render*
  [^Mustache template context]
  (with-out-str
    (let [scope (HashMap. (walk/stringify-keys context))]
      (.execute template *out* scope))))

(defn render-string
  "Render string as mustache template."
  ([^String template]
   (render-string template {}))
  ([^String template context]
   (let [reader (StringReader. template)
         template (.compile +mustache-factory+ reader "example")]
     (render* template context))))

(defn render
  "Load a file from the class path and render
  it using mustache template."
  ([^String path]
   (render path {}))
  ([^String path context]
   (render-string (slurp (io/resource path)) context)))
