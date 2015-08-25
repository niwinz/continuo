(ns continuo.postgresql-spec
  (:require [clojure.test :as t]
            [continuo.postgresql.schema :as schema]))

(t/deftest schema-internal-tests
  (let [schema [[:db/add :user/username {:type :continuo/string}]
                [:db/add :user/fullname {:type :continuo/string}]
                [:db/drop :user/name]]
        cschema (schema/compile-schema schema)]
    (cschema (fn [sql]
               (println "Executing:" sql)))))
