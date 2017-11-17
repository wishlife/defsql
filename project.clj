(defproject defsql "0.1.7-SNAPSHOT"
  :description "A Clojure library designed to make your life easier with SQL Database using JDBC."
  :url "https://github.com/wishlife/defsql"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]]
  :profiles {:dev {:dependencies [[org.postgresql/postgresql "9.4-1205-jdbc41"]
                                  [org.clojure/java.jdbc "0.4.2"]
                                  [cheshire "5.5.0"]]}}
  :deploy-repositories  [["releases" :clojars]]
  :lein-release {:deploy-via :clojars})
