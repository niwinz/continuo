(ns continuo.postgresql-spec
  (:require [clojure.test :as t]
            [promissum.core :as p]
            [suricatta.core :as sc]
            [continuo.core :as co]
            [continuo.impl :as impl]
            [continuo.executor :as exec]
            [continuo.util.exceptions :refer [unwrap-exception]]
            [continuo.postgresql.transaction :as tx]
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

(t/deftest apply-schema-test
  (let [schema [[:db/add :user/username {:type :string}]]
        _      (p/await (co/create uri))
        conn   (p/await (co/open uri))
        result (p/await (co/run-schema conn schema))]
    (t/is (= result
             {:user/username {:type :string, :ident :user/username}}))))

(t/deftest mkeid-handling-test
  (binding [impl/*eid-map* (volatile! {})]
    (let [r1 (impl/mkeid 1)
          r2 (impl/mkeid 2)
          r3 (impl/mkeid 1)]
      (t/is (= (impl/-resolve-eid r1)
               (impl/-resolve-eid r1)))
      (t/is (= (impl/-resolve-eid r1)
               (impl/-resolve-eid r3)))
      (t/is (not= (impl/-resolve-eid r2)
                  (impl/-resolve-eid r3))))))

(t/deftest simple-transact-with-tmpeid
  (p/await (co/create uri))
  (let [conn (p/await (co/open uri))]

    ;; Create schema
    (let [schema [[:db/add :foo/bar {:type :string}]
                  [:db/add :foo/baz {:type :integer}]]]
      (p/await (co/run-schema conn schema)))

    (let [facts [[:db/add (co/mkeid 1) :foo/bar "hello world"]
                 [:db/add (co/mkeid 1) :foo/baz 67]]
          res (p/await (co/transact conn facts))]
      (println res)
      (t/is (set? res))
      (t/is (= (count res) 1)))))

(t/deftest simple-transact-with-retract
  (p/await (co/create uri))
  (let [conn (p/await (co/open uri))]

    ;; Create schema
    (let [schema [[:db/add :foo/bar {:type :string}]
                  [:db/add :foo/baz {:type :integer}]]]
      (p/await (co/run-schema conn schema)))

    (let [facts [[:db/add (co/mkeid 1) :foo/bar "hello world"]
                 [:db/add (co/mkeid 1) :foo/baz 67]
                 [:db/retract (co/mkeid 1) :foo/baz 67]]
          res (p/await (co/transact conn facts))]
      (println res)
      (t/is (set? res))
      (t/is (= (count res) 1)))))

