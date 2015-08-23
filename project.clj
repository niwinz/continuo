(defproject funcool/continuo "0.1.0-SNAPSHOT"
  :description "A continuous transaction log persistence for Clojure."
  :url "https://github.com/funcool/continuo"
  :license {:name "Apache 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0.txt"}
  :javac-options ["-target" "1.8" "-source" "1.8" "-Xlint:-options"]
  :dependencies [[org.clojure/clojure "1.7.0" :scope "provided"]
                 [com.taoensso/nippy "2.9.0"]
                 [funcool/cats "0.6.1"]
                 [funcool/cuerdas "0.6.0"]
                 [funcool/promissum "0.2.0"]
                 [funcool/suricatta "0.3.1"]
                 [hikari-cp "1.3.0"]
                 [com.cognitect/transit-clj "0.8.281"]
                 [cheshire "5.5.0"]]
  :plugins [[lein-ancient "0.6.7"]]
  :profiles {:dev {:global-vars {*warn-on-reflection* true}}})

