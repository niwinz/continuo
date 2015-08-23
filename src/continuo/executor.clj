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

(ns continuo.executor
  "A basic abstraction for executor services."
  (:require [promissum.core :as p])
  (:import java.util.concurrent.ForkJoinPool
           java.util.concurrent.Executor
           java.util.concurrent.Executors
           java.util.concurrent.ThreadFactory))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; The main abstraction definition.
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defprotocol IExecutor
  (^:private execute* [_ task] "Execute a task in a executor."))

(defprotocol IExecutorService
  (^:private submit* [_ task] "Submit a task and return a promise."))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Implementation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(extend-type Executor
  IExecutor
  (execute* [this task]
    (.execute this ^Runnable task))

  IExecutorService
  (submit* [this task]
    (let [promise (p/promise)]
      (execute* this (fn []
                       (try
                         (p/deliver promise (task))
                          (catch Throwable e
                            (p/deliver promise e)))))
      promise)))

(defn- thread-factory-adapter
  "Adapt a simple clojure function into a
  ThreadFactory instance."
  [func]
  (reify ThreadFactory
    (^Thread newThread [_ ^Runnable runnable]
      (func runnable))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public Api
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^:dynamic *default* (ForkJoinPool/commonPool))
(def ^:dynamic *default-thread-factory* (Executors/defaultThreadFactory))

(defn fixed
  "A fixed thread pool constructor."
  ([n]
   (Executors/newFixedThreadPool n *default-thread-factory*))
  ([n factory]
   (Executors/newFixedThreadPool n (thread-factory-adapter factory))))

(defn single-thread
  "A single thread executor constructor."
  ([]
   (Executors/newSingleThreadExecutor *default-thread-factory*))
  ([factory]
   (Executors/newSingleThreadExecutor (thread-factory-adapter factory))))

(defn cached
  "A cached thread executor constructor."
  ([]
   (Executors/newCachedThreadPool *default-thread-factory*))
  ([factory]
   (Executors/newCachedThreadPool (thread-factory-adapter factory))))

(defn execute
  "Execute a task in a provided executor.

  A task is a plain clojure function or
  jvm Runnable instance."
  ([task]
   (execute* *default* task))
  ([executor task]
   (execute* executor task)))

(defn submit
  "Submit a task to be executed in a provided executor
  and return a promise that will be completed with
  the return value of a task.

  A task is a plain clojure function."
  ([task]
   (submit* *default* task))
  ([executor task]
   (submit* executor task)))

