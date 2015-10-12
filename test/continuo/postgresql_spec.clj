(ns continuo.postgresql-spec
  (:require [clojure.test :as t]
            [suricatta.core :as sc]
            [continuo.core :as co]
            [continuo.impl :as impl]
            [continuo.executor :as exec]
            [continuo.util.exceptions :refer [unwrap-exception]]
            [continuo.postgresql.bootstrap :as boot]
            [continuo.postgresql.connection :as conn]
            [continuo.postgresql.schema :as schema])
  (:import java.net.URI))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Fixtures & Initialization
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def +ctx+)
(def dbspec {:subprotocol "postgresql"
             :subname "//127.0.0.1/test"})

(defn transaction-fixture
  [continuation]
  (with-open [ctx (sc/context dbspec)]
    (sc/atomic ctx
      (alter-var-root #'+ctx+ (constantly ctx))
      (continuation)
      (sc/set-rollback! ctx))))

(deftype Transactor [schema]
  conn/ITrasactor
  (-get-connection [_] +ctx+)
  (-get-schema [_] schema)

  impl/ITransactorInternal
  (-initialize [it] (boot/initialize it))
  (-create [it] (boot/create it)))


(defmethod impl/connect :pgtest
  [_ _]
  (exec/submit (fn [] (Transactor. (atom nil)))))

(t/use-fixtures :each transaction-fixture)

(def ^:static uri "pgtest://localhost/test")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Tests
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(t/deftest facked-connect-tests
  (let [tx @(impl/connect (URI. uri) {})]
    (t/is (instance? Transactor tx))))

(t/deftest open-not-created-database
  (try
    @(co/open uri {})
    (throw (ex-info "unexected" {}))
    (catch java.util.concurrent.ExecutionException e
      (let [e (unwrap-exception e)]
        (t/is (instance? clojure.lang.ExceptionInfo e))
        (t/is (= (ex-data e) {:type :error/db-not-initialized}))))))
