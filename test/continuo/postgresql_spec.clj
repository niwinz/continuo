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
      (alter-var-root #'conn/+test-connection+ (constantly ctx))
      (continuation)
      (alter-var-root #'+ctx+ (constantly nil))
      (alter-var-root #'conn/+test-connection+ (constantly nil))
      (sc/set-rollback! ctx))))

(t/use-fixtures :each transaction-fixture)

(def ^:static uri "postgresql-test://localhost/test")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Tests
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(t/deftest facked-connect-tests
  (let [tx @(impl/connect (URI. uri) {})]
    (t/is (instance? continuo.postgresql.connection.TestTransactor tx))))

(t/deftest open-not-created-database
  (try
    @(co/open uri {})
    (throw (ex-info "unexected" {}))
    (catch java.util.concurrent.ExecutionException e
      (let [e (unwrap-exception e)]
        (t/is (instance? clojure.lang.ExceptionInfo e))
        (t/is (= (ex-data e) {:type :error/db-not-initialized}))))))

(t/deftest create-database-test
  (let [created? @(co/create uri)]
    (t/is created?))
  (let [created? @(co/create uri)]
    (t/is (not created?))))
