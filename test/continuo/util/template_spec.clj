(ns continuo.util.template-spec
  (:require [clojure.test :as t]
            [continuo.util.template :as tmpl]))

(t/deftest mustache-template-tests
  (t/testing "Reader simple string with var interpolation."
    (t/is (= "Hello world!"
             (tmpl/render-string "Hello {{ name }}!" {:name "world"})))))
