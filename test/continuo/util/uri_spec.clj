(ns continuo.util.uri-spec
  (:require [clojure.test :as t]
            [continuo.util.uri :as uri])
  (:import java.net.URI))

(t/deftest uri-coersions-test
  (t/is (= (URI/create "http://foo/bar")
           (uri/-coerce "http://foo/bar")))

  (let [uri (URI/create "http://foo/bar")]
    (t/is (identical? uri (uri/-coerce uri)))))

(t/deftest querystring-to-map-test
  (let [qs1 "foo=1&bar=2"
        qs2 "fooBar=1.1&barBaz=foo&a=:b&c=#{1 2}"
        rs1 (uri/querystring->map qs1)
        rs2 (uri/querystring->map qs2)]
    (t/is (= rs1 {:foo 1 :bar 2}))
    (t/is (= rs2 {:foo-bar 1.1 :bar-baz "foo"
                  :a :b :c #{1 2}}))))

(t/deftest parse-uri-to-params-test
  (let [uri (URI/create "pg://localhost:5432/dbname?param1=1&param2=2")
        res (uri/parse uri)]
    (t/is (= res
             {:host "localhost"
              :params {:param1 1
                       :param2 2}
              :port 5432
              :path "dbname"}))))

