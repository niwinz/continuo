(defproject funcool/continuo "0.1.0-SNAPSHOT"
  :description "A continuous transaction log persistence for Clojure."
  :url "https://github.com/funcool/continuo"
  :license {:name "Apache 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0.txt"}
  :javac-options ["-target" "1.8" "-source" "1.8" "-Xlint:-options"]
  :dependencies [[org.clojure/clojure "1.7.0" :scope "provided"]
                 [com.taoensso/nippy "2.10.0"]
                 [funcool/cats "1.1.0-SNAPSHOT"]
                 [funcool/cuerdas "0.6.0"]
                 [funcool/promissum "0.3.1"]
                 [funcool/suricatta "0.4.0-SNAPSHOT"]
                 [com.h2database/h2 "1.4.189"]
                 [org.postgresql/postgresql "9.4-1202-jdbc42"]
                 [hikari-cp "1.3.1"]
                 [cheshire "5.5.0"]
                 [com.github.spullara.mustache.java/compiler "0.9.1"]]
  :plugins [[lein-ancient "0.6.7"]])
  ;; :profiles {:dev {:global-vars {*warn-on-reflection* false}}})

