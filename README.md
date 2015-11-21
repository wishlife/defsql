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

#### Cheshire

    [cheshire "5.5.0"]

    When json type is used.


### Prepare database connection

```clojure
(require '[defsql.core :refer [defquery defupdate definsert]])
(require '[clojure.java.jdbc :as jdbc])
```

;; Define a database connection spec.
```clojure
(def db-spec {:classname "org.postgresql.Driver"
              :subprotocol "postgresql"
              :subname "//localhost:5432/demo"
              :user "test"
              :password "pwd"})
```

### Database DDL
; Create database table
```clojure
(defupdate create-test-table []
  "create table demo_test (id bigserial not null, name text not null, country text)")

(jdbc/with-db-connection [db-conn db-spec]
  (create-test-table (:connection db-conn)))
```

; Drop database table
```clojure
(defupdate drop-test-table []
  "drop table demo_test")

(jdbc/with-db-connection [db-conn db-spec]
  (drop-test-table (:connection db-conn)))
```

### Database DML

; Insert Data
```clojure
(definsert insert-test-data []
  "insert into demo_test (name, country) values
   ('John', 'US'),
   ('Charlie', 'US'),
   ('Albert', 'UK'),
   ('Bob', null),
   ('Lin', 'CHN')")

(jdbc/with-db-connection [db-conn db-spec]
  (insert-test-data (:connection db-conn)))
```

; Define query
```clojure
(defquery users-by-country [country]
  "SELECT * FROM demo_test WHERE country = :country")
```

; Execute query
```clojure
(jdbc/with-db-connection [db-conn db-spec]
  (users-by-country (:connection db-conn) "US"))
```

; Update
```clojure
(defupdate update-test-data [name country]
  "update demo_test set country = :country where name = :name")

(jdbc/with-db-connection [db-conn db-spec]
  (update-test-data (:connection db-conn) "John" "UK"))
```

### Testing

defsql.defsql-check/defsql-tests can generate test cases for all SQL statement automatically.

```clojure
(ns sandbox-defsql.core-test
  (:require [clojure.test :refer :all]
            [sandbox-defsql.core :refer :all]
            [defsql.defsql-check :refer [defsql-tests]]
            [clojure.java.jdbc :as jdbc]))

(defn- open-test-connection []
  (jdbc/get-connection db-spec))

;; defsql-tests will create test cases for all SQL statement, test
;; cases will run using database connection provided by
;; open-test-connection function
(defsql-tests open-test-connection sandbox-defsql.core)

```

## License

Copyright © 2015 Wishlife Inc.

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
