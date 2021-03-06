(ns defsql.defsql-check
  (:require [clojure.test :as test]))

(def ^:private tag-to-sql-type
  {String "varchar"

   Integer "int4"
   Integer/TYPE "int4"
   Long "int8"
   Long/TYPE "int8"
   Float "float4"
   Float/TYPE "float4"
   Double "float8"
   Double/TYPE "float8"
   Short "int2"
   Short/TYPE "int2"
   Byte "int2" ;; no int1
   Byte/TYPE "int2"
   Character "int2"
   Character/TYPE "int2"
   Boolean "bool" ;; or "bit"
   Boolean/TYPE "bool"
   
   (Class/forName "[B") "bytea"

   ;; ??? java.sql.Array
   java.math.BigDecimal "numeric"
   java.sql.Blob "bytea"
   java.sql.Clob "text"
   java.sql.Date "date"
   java.sql.NClob "text"
   ;; ??? java.sql.Ref
   ;; ??? java.sql.RowId
   ;; ??? java.sql.SQLXML
   java.sql.Time "time"
   java.sql.Timestamp "timestamp"
   ;; ??? java.net.URL 

   clojure.lang.Symbol "varchar"
   clojure.lang.Keyword "varchar"})

(defn- pmeta-to-sql-type
  "Returns the Postgres-specific sql type given the metadata on a
  parameter."
  [pmeta]
  (if-let [atype (:array pmeta)]
    (str atype "[]")
    (if (:json pmeta)
      "varchar"
      (or (if-let [tag (:tag pmeta)]
            (get tag-to-sql-type (resolve tag)))
          "unknown"))))

(defn- prepare-sql
  "Converts a JDBC prepared statement to a Postgres prepared
  statement.  JDBC uses implicit positional parameters marked by a
  '?', whereas postgres uses explicit positional parameters in the
  form '$n' where n is the 1-based index of the argument (i.e., '$1'
  for the first argument)."
  [stmt-name arglist bindorder sql]
  (let [pmeta (->> (next arglist) ;; first argument is the connection
                   (map #(vector (str %) (meta %)))
                   (into {}))
        sbuf (StringBuffer.)]
    (loop [args []
           m (re-matcher #"\?" sql)
           argno 0]
      (if (re-find m)
        (recur (conj args (pmeta-to-sql-type (pmeta (bindorder argno))))
               (.appendReplacement m sbuf (str "\\$" (inc argno)))
               (inc argno))
        (str "PREPARE " stmt-name "("
             (clojure.string/join "," args)
             ") AS\n" (.appendTail m sbuf))))))

(defn check-defsql
  "Checks a query by creating a Postgres prepared statement.  This
  prepares (then deallocates) a statement in the Postgres session.  In
  preparing, postgres performs analysis on the statement in order to
  make it execute faster when called--this analysis will throw an
  error if the SQL is incorrect, has invalid references, or has
  parameters bound to the wrong type.  Basically this is a very good
  sanity check.  (Note: the postgres JDBC driver uses prepared
  statements under the hood of a JDBC PreparedStatement, after a
  threshold of executions.)"
  [open-connection sqlfn]
  (let [m (meta sqlfn)
        ;; give the prepared statement an identifiable name.  It does
        ;; not need to be unique since it is immediately deallocated.
        stmt-name (str "test_" (munge (:name m)))
        ;; extract the JDBC prepared statement's sql and binding order.
        bindorder (:defsql.core/bindorder m)
        bindsql (:defsql.core/bindsql m)
        ;; convert to a postgres 'PREPARE'.
        prepare (prepare-sql stmt-name (first (:arglists m)) bindorder bindsql)
        ;; PREPARE and then DEALLOCATE the statement, and convert a
        ;; thrown exception to an error message.
        ;; err ;;(jdbc/with-db-transaction [db-conn (:db-pool (get-state))]
        err (with-open [db-conn (open-connection)
                        stmt (.createStatement db-conn)] ;;(:connection db-conn))]
                (try
                  (.executeUpdate stmt prepare)
                  (.executeUpdate stmt (str "DEALLOCATE " stmt-name))
                  nil ;; err = nil
                  (catch java.sql.SQLException ex
                    (.getMessage ex))))]
    ;; (println "CHECKING: " prepare)
    (test/is (nil? err))))

(defmacro defsql-tests
  "Creates a test for each function generated by defsql in the
  namespace."
  [open-connection ns]
  `(do
     ~@(for [sqlfn (vals (ns-interns ns))
             :when (:defsql.core/bindsql (meta sqlfn))]
         `(test/deftest ~(symbol (str "test-" (:name (meta sqlfn))))
            (check-defsql ~open-connection ~sqlfn)))))


