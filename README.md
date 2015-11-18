# defsql

A Clojure library designed to make your life easier with SQL Database using JDBC.

## Usage

### Installation

Add this to your [Leiningen](https://github.com/technomancy/leiningen) :dependencies:

[![Clojars Project](http://clojars.org/defsql/latest-version.svg)](http://clojars.org/defsql)

### Dependencies

#### JDBC Driver

    [org.postgresql/postgresql "9.4-1205-jdbc41"]

#### clojure.java.jdbc

    [org.clojure/java.jdbc "0.4.2"]

    Use for database connection and transaction management.


### Prepare database connection

(require '[defsql.core :refer [defquery]])
(require '[clojure.java.jdbc :as jdbc])

;; Define a database connection spec.
```clojure
(def db-spec {:classname "org.postgresql.Driver"
              :subprotocol "postgresql"
              :subname "//localhost:5432/demo"
              :user "test"
              :password "pwd"})
```

;; Define query
```clojure
(defquery users-by-country [country]
  "SELECT * FROM demo_user WHERE country = :country")
```

;; Execute query
```clojure
(jdbc/with-db-connection [db-conn db-spec]
  (users-by-country (:connection db-conn) "US"))
```

## License

Copyright © 2015 Wishlife Inc.

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
