(ns defsql.core)

(defn- check-arg
  ([chk msg]
   (or chk
       (throw (IllegalArgumentException. msg))))
  ([chk fmt & args]
   (or chk
       (throw (IllegalArgumentException. (apply format fmt args))))))

(defn- check-empty
  [msg items]
  (or (empty? items)
      (throw (IllegalArgumentException. (str msg ": " (clojure.string/join ", " items))))))

(def ^:private sql-re
  #"(?xs: # turn on comments and dot-all

    # comments (we keep the line terminator for error messages)
      \s*--.*?([\r\n]|$)

    # strings
    | ( \' (?:[^\']|\\.)+ \'
      | \" (?:[^\"]|\\.)+ \")

    # symbols (negative lookbehind to prevent matching ::casts)
    | (?<!:) : ([a-zA-Z] [a-zA-Z0-9\*\+!\-_\?] *)

    # optional.namespace / *dynamic-variable*
    | (?: ( [a-zA-Z] [a-zA-Z0-9\*\+!\-_\?]*
          (?: \. [a-zA-Z] [a-zA-Z0-9\*\+!\-_\?]* )* ) / )?
      (\* [a-zA-Z] [a-zA-Z0-9*+!\-_?]* \*)

    # unnamed parameters (not supported currently)
    | (\?)
    )")

(defn- map-sql-params
  "Converts a SQL string with named parameters to a string suitable
  for use with JDBC positional parameters.  It strips out comments,
  while leaving newlines (for consistent error messages), and ignores
  things that look like parameters that are not (string contents, and
  SQL type casts.)

  The return value is a vector of [bindorder varset sql], where
  bindorder is the name parameter vector in binding order, varset is a
  set of references variables, and sql is the SQL string to use in a
  prepared statement."
  [ns sql]
  (let [sb (StringBuffer.)]
    (loop [bindorder [] varset #{} m (re-matcher sql-re sql)]
      (if-let [[_ comment string param opt-ns dvar unnamed] (re-find m)]
        (cond
          comment (recur bindorder varset (.appendReplacement m sb (clojure.string/re-quote-replacement comment)))
          string (recur bindorder varset (.appendReplacement m sb (clojure.string/re-quote-replacement string)))
          param (recur (conj bindorder (symbol param)) varset (.appendReplacement m sb "?"))
          dvar (let [v (if opt-ns (symbol opt-ns dvar) (symbol dvar))]
                 (recur (conj bindorder v) (conj varset v) (.appendReplacement m sb "?")))
          unnamed (throw (IllegalArgumentException.
                          "Unnamed parameters (?) are not supported (currently)")))
        [bindorder varset (str (.appendTail m sb))]))))

(defn set-nullable-long*
  "Helper for setting a nullable long value"
  [^java.sql.PreparedStatement ps ^Integer idx ^Long l]
  (if l
    (.setLong ps idx l)
    (.setNull ps idx java.sql.Types/BIGINT)))

(defn set-nullable-integer*
  "Helper for setting a nullable integer value"
  [^java.sql.PreparedStatement ps ^Integer idx ^Integer i]
  (if i
    (.setInt ps idx i)
    (.setNull ps idx java.sql.Types/INTEGER)))

(defn set-nullable-float*
  "Helper for setting a nullable float value"
  [^java.sql.PreparedStatement ps ^Integer idx ^Float f]
  (if f
    (.setFloat ps idx f)
    (.setNull ps idx java.sql.Types/FLOAT)))

(defn set-nullable-double*
  [^java.sql.PreparedStatement ps ^Integer idx ^Double d]
  (if d
    (.setDouble ps idx d)
    (.setNull ps idx java.sql.Types/DOUBLE)))

(defn set-nullable-short*
  [^java.sql.PreparedStatement ps ^Integer idx ^Short s]
  (if s
    (.setShort ps idx s)
    (.setNull ps idx java.sql.Types/SMALLINT)))

(defn set-nullable-byte*
  [^java.sql.PreparedStatement ps ^Integer idx ^Byte b]
  (if b
    (.setByte ps idx b)
    (.setNull ps idx java.sql.Types/TINYINT)))

(defn set-nullable-boolean*
  [^java.sql.PreparedStatement ps ^Integer idx ^Boolean b]
  (if-not (nil? b)
    (.setBoolean ps idx b)
    (.setNull ps idx java.sql.Types/BOOLEAN)))

;; (defn bind-keyword
;;   [^java.sql.PreparedStatement ps index ^clojure.lang.Keyword value]
;;   (.setString ps index (name value)))

;; refer to clojure/lang/Compiler.java
;;   tagToClass
;; clojure.lang.genclass/the-class (private)

(def ^:private ^:const byte-array-class
  (Class/forName "[B"))

(def ^:private primitive-tag->class
  {'objects (Class/forName "[Ljava.lang.Object;")
   'ints (Class/forName "[I")
   'longs (Class/forName "[J")
   'floats (Class/forName "[F")
   'doubles (Class/forName "[D")
   'chars (Class/forName "[C")
   'shorts (Class/forName "[S")
   'bytes byte-array-class
   'booleans (Class/forName "[Z")
   'int Integer/TYPE
   'long Long/TYPE
   'float Float/TYPE
   'double Double/TYPE
   'char Character/TYPE
   'short Short/TYPE
   'byte Byte/TYPE
   'boolean Boolean/TYPE})

(def ^:private extension-tag->class
  {'keyword clojure.lang.Keyword
   'symbol clojure.lang.Symbol})

(defn- str-to-class
  [^String s]
  (Class/forName
   (if (re-find #"[\.\[]" s)
     s
     (str "java.lang." s))))

(defn- tag-to-class
  [tag]
  (cond
    (nil? tag) nil
    (class? tag) tag
    :else (or (primitive-tag->class tag)
              (extension-tag->class tag)
              (str-to-class (str tag)))))

(defmulti bind-parameter
  "bind-parameter provides a way to bind parameters differently based
  upon their tag.  This allows type-specific setters to be used, such
  as .setString, instead of the generic .setObject.  When no other
  binding type is available it uses .setObject."
  (fn [pmeta index param]
    (tag-to-class (:tag pmeta))))

(defmethod bind-parameter String [_ index param] (list '.setString index param))

(defmethod bind-parameter Integer/TYPE [_ index param] (list '.setInt index param))
(defmethod bind-parameter Long/TYPE [_ index param] (list '.setLong index param))
(defmethod bind-parameter Float/TYPE [_ index param] (list '.setFloat index param))
(defmethod bind-parameter Double/TYPE [_ index param] (list '.setDouble index param))
(defmethod bind-parameter Short/TYPE [_ index param] (list '.setShort index param))
(defmethod bind-parameter Byte/TYPE [_ index param] (list '.setByte index param))
(defmethod bind-parameter Character/TYPE [_ index param] (list '.setInt index param))
(defmethod bind-parameter Boolean/TYPE [_ index param] (list '.setBoolean index param))

(defmethod bind-parameter Integer [_ index param] (list `set-nullable-integer* index param))
(defmethod bind-parameter Long [_ index param] (list `set-nullable-long* index param))
(defmethod bind-parameter Float [_ index param] (list `set-nullable-float* index param))
(defmethod bind-parameter Double [_ index param] (list `set-nullable-double* index param))
(defmethod bind-parameter Short [_ index param] (list `set-nullable-short* index param))
(defmethod bind-parameter Byte [_ index param] (list `set-nullable-byte* index param))
(defmethod bind-parameter Character [_ index param] (list `set-nullable-integer* index param))
(defmethod bind-parameter Boolean [_ index param] (list `set-nullable-boolean* index param))

(defmethod bind-parameter java.sql.Array [_ index param] (list '.setArray index param))
(defmethod bind-parameter java.math.BigDecimal [_ index param] (list '.setBigDecimal index param))
(defmethod bind-parameter java.sql.Blob [_ index param] (list '.setBlob index param))
(defmethod bind-parameter byte-array-class [_ index param] (list '.setBytes index param))
(defmethod bind-parameter java.sql.Clob [_ index param] (list '.setClob index param))
(defmethod bind-parameter java.sql.Date [_ index param] (list '.setDate index param))
(defmethod bind-parameter java.sql.NClob [_ index param] (list '.setNClob index param))
(defmethod bind-parameter java.sql.Ref [_ index param] (list '.setRef index param))
(defmethod bind-parameter java.sql.RowId [_ index param] (list '.setRowId index param))
(defmethod bind-parameter java.sql.SQLXML [_ index param] (list '.setSQLXML index param))
(defmethod bind-parameter java.sql.Time [_ index param] (list '.setTime index param))
(defmethod bind-parameter java.sql.Timestamp [_ index param] (list '.setTimestamp index param))
(defmethod bind-parameter java.net.URL [_ index param] (list '.setURL index param))

(defmethod bind-parameter clojure.lang.Keyword [_ index param] (list '.setString index `(if ~param (name ~param))))
(defmethod bind-parameter clojure.lang.Symbol [_ index param] (list '.setString index `(if ~param (name ~param))))
(defmethod bind-parameter :default [_ index param] (list '.setObject index param))

;; TODO: warn about unmapped types that are common mistakes, e.g.,
;; java.util.Date?

(defmulti get-field (fn [fclass rs index] fclass))

(defmethod get-field String [_ rs index] (list '.getString rs index))

(defmethod get-field Integer/TYPE [_ rs index] (list '.getInt rs index))
(defmethod get-field Long/TYPE [_ rs index] (list '.getLong rs index))
(defmethod get-field Float/TYPE [_ rs index] (list '.getFloat rs index))
(defmethod get-field Double/TYPE [_ rs index] (list '.getDouble rs index))
(defmethod get-field Short/TYPE [_ rs index] (list '.getShort rs index))
(defmethod get-field Byte/TYPE [_ rs index] (list '.getByte rs index))
;; The is no (.getChar), use (.getInteger) instead.  Note: (.getShort)
;; might have problems since char is unsigned.
(defmethod get-field Character/TYPE [_ rs index] `(char (~'.getInteger ~rs ~index)))
;; Use (= v# true) to guarantee our results are the Boolean/TRUE and
;; Boolean/FALSE singletons.  The JDBC driver may have used the
;; Boolean constructor.
(defmethod get-field Boolean/TYPE [_ rs index] `(= (~'.getBoolean ~rs ~index) true))

(defn- get-nullable-primitive
  [method rs index]
  `(let [v# ~(list method rs index)]
     (if-not ~(list '.wasNull rs) v#)))

(defmethod get-field Integer [_ rs index] (get-nullable-primitive '.getInt rs index))
(defmethod get-field Long [_ rs index] (get-nullable-primitive '.getLong rs index))
(defmethod get-field Float [_ rs index] (get-nullable-primitive '.getFloat rs index))
(defmethod get-field Double [_ rs index] (get-nullable-primitive '.getDouble rs index))
(defmethod get-field Short [_ rs index] (get-nullable-primitive '.getShort rs index))
(defmethod get-field Byte [_ rs index] (get-nullable-primitive '.getByte rs index))
;; The is no (.getChar), use (.getInteger) instead.  Note: (.getShort)
;; might have problems since char is unsigned.
(defmethod get-field Character [_ rs index] `(let [i# ~(list '.getInteger rs index)]
                                               (if-not ~(list '.wasNull rs) (char i#))))
;; Use (= v# true) to guarantee our results are the Boolean/TRUE and
;; Boolean/FALSE singletons.  The JDBC driver may have used the
;; Boolean constructor.
(defmethod get-field Boolean [_ rs index] `(let [b# (~'.getBoolean ~rs ~index)]
                                             (if-not (~'.wasNull ~rs) (= b# true))))

(defmethod get-field java.sql.Array [_ rs index] (list '.getArray rs index))
;; TODO: vector/list/seq/other? -> (.getArray ...)
;; TODO: InputStream -> getAsciiStream (conflicts)
(defmethod get-field java.math.BigDecimal [_ rs index] (list '.getBigDecimal rs index))
;; TODO: InputStream -> getBinaryStream (conflicts)
(defmethod get-field java.sql.Blob [_ rs index] (list '.getBlob rs index))
(defmethod get-field byte-array-class [_ rs index] (list '.getBytes rs index))
;; TODO: Reader -> CharacterStream (conflicts)
(defmethod get-field java.sql.Clob [_ rs index] (list '.getClob rs index))
;; TODO: there is also a two-argument version that takes a Calandar
(defmethod get-field java.sql.Date [_ rs index] (list '.getDate rs index))
;; TODO: Reader -> getNCharacterStream (conflicts)
(defmethod get-field java.sql.NClob [_ rs index] (list '.getNClob rs index))
;; TODO: String -> NString (conflicts, and getString is much more common)
(defmethod get-field java.sql.Ref [_ rs index] (list '.getRef rs index))
(defmethod get-field java.sql.RowId [_ rs index] (list '.getRowId rs index))
(defmethod get-field java.sql.SQLXML [_ rs index] (list '.getSQLXML rs index))
;; TODO: there is also a two-argument version that takes a Calandar
(defmethod get-field java.sql.Time [_ rs index] (list '.getTime rs index))
;; TODO: there is also a two-argument version that takes a Calandar
(defmethod get-field java.sql.Timestamp [_ rs index] (list '.getTimestamp rs index))
(defmethod get-field java.net.URL [_ rs index] (list '.getURL rs index))

(defmethod get-field clojure.lang.Keyword [_ rs index]
  `(if-let [k# (~'.getString ~rs ~index)] (keyword k#)))

(defmethod get-field clojure.lang.Symbol [_ rs index]
  `(if-let [s# (~'.getString ~rs ~index)] (symbol s#)))


(defmethod get-field :default [tag rs index]
  (if tag
    (list '.getObject rs index tag)
    (list '.getObject rs index)))

;; TODO: special values in ResultSet
;; - (.getRow rs) = current row number

(defn column-labels
  "Extracts the column labels from a result set to be used as keys in
  row maps.  This function pairs with the row-to-map."
  [^java.sql.ResultSet rs]
  (let [md (.getMetaData rs)
        column-count (.getColumnCount md)]
    (loop [labels (transient [])
           ;; keep track of label counts to disambiguate columns with
           ;; the same name with a suffix. E.g., :foo, :foo_2, :foo_3, ...
           counts (transient {})
           i 1]
      (if (<= i column-count)
        (let [label (-> (.getColumnLabel md i)
                        (clojure.string/lower-case))
              ;; if we wanted to be consistent with clojure naming
              ;; conventions, we might consider converting '_' to '-',
              ;; as in user_name becomes user-name.
              ;;        (.replace \_ \-))
              count (inc (get counts label 0))]
          (recur (conj! labels
                        (keyword (if (= 1 count)
                                   label
                                   (str label "_" count))))
                 (assoc! counts label count)
                 (inc i)))
        (persistent! labels)))))

(defn row-to-map
  "Converts a row to a map, using the labels vector for keys."
  [labels ^java.sql.ResultSet rs]
  (let [column-count (count labels)]
    (loop [r (transient {}) i 0]
      (if (< i column-count)
        (let [k (labels i)
              i (inc i)
              v (.getObject rs i)
              v (cond (.wasNull rs) nil
                      (instance? Boolean v) (= v true)
                      :else v)]
          (recur (assoc! r k v) i))
        (persistent! r)))))

(defn read-rows
  "The default defquery row reader.  This function iterates through
  all rows in the result set and returns a vector with one entry per
  row.  The `rowfn' parameter, if present is used to convert a row to
  a record in the resulting vector.  The single argument version uses
  the metadata from the result set to create a map for each entry."
  ([^java.sql.ResultSet rs]
   (read-rows rs (partial row-to-map (column-labels rs))))
   ;; (let [labels (column-labels rs)]
   ;;   (loop [rows (transient [])]
   ;;     (if (.next rs)
   ;;       (recur (conj! rows (row-to-map labels rs)))
   ;;       (persistent! rows)))))
  
  ([^java.sql.ResultSet rs rowfn]
   (loop [rows (transient [])]
     (if (.next rs)
       (recur (conj! rows (rowfn rs)))
       (persistent! rows)))))

(defn read-first-row
  ([^java.sql.ResultSet rs]
   (read-first-row rs (partial row-to-map (column-labels rs))))

  ([^java.sql.ResultSet rs rowfn]
   (if (.next rs)
     (rowfn rs))))

(defn read-single-row
  ([^java.sql.ResultSet rs]
   (read-single-row rs (partial row-to-map (column-labels rs))))
  ([^java.sql.ResultSet rs rowfn]
   (if-not (.next rs)
     (throw (java.sql.SQLException. "no result"))
     (let [result (rowfn rs)]
       (if (.next rs)
         (throw (java.sql.SQLException. "non-unique result"))
         result)))))

;; TODO:
;; - Add support for user provided row-mapper, e.g.,
;;   (defquery name-of-query
;;      [params ...]
;;      "sql ..."
;;      rs-to-vector)
;; - Add ability to cache result-set metadata, e.g., transform to:
;;   (def name-of-query
;;     (let [rsmeta# (atom)] (fn [...] ... (read-rows rsmeta# rs#)))
;;   in rsmeta# would do a compare-and-set! on first call
;; - Add additional settings as to defn, e.g., doc-string, '-' for private, prepost-map?, attr-map?

(defn- bind-parameters
  [bindorder pmeta opts]
  (for [i (range 0 (count bindorder))
        :let [p (bindorder i)]
        :while (check-arg (contains? pmeta p) "SQL reference to ':%s' not found in parameter list" p)]
    (bind-parameter (pmeta p) (inc i) p)))

(defn- execute-query
  "Executor for defquery.  It prepares, binds, executes the query,
  then iterates through the ResultSet."
  [bindorder bindsql pmeta c opts]
  ;; it is tempting to put the doto with parameters on the
  ;; prepared statement binding since doto returns the first
  ;; argument, e.g.:
  ;;  ps# (doto (.prepareStatement ~c ~bindsql)
  ;;            (.setObject 1 ...))
  ;; however, if binding a parameter results in an exception, we
  ;; wouldn't properly close the statement.
  (check-empty "unknown option" (-> (dissoc opts :row :rows) keys))
  (let [rs `rs#
        reader (case (:rows opts)
                 :first `read-first-row
                 :single `read-single-row
                 nil `read-rows)]
    `(with-open [ps# (.prepareStatement ~c ~bindsql)
                 ~rs (.executeQuery
                      (doto ps#
                        ~@(bind-parameters bindorder pmeta opts)
                        ~@(case (:rows opts)
                            :first [(list '.setMaxRows 1)]
                            :single [(list '.setMaxRows 2)]
                            nil)))]
       ~(cond
          (:row opts)
          (list reader rs (:row opts))

          :else
          (list reader rs)))))

(defn- execute-update
  "Executor for defupdate.  It prepares, binds, and calls
  executeUpdate."
  [bindorder bindsql pmeta c opts]
  (check-empty "unknown options" (keys opts))
  `(with-open [ps# (.prepareStatement ~c ~bindsql)]
     (.executeUpdate (doto ps#
                       ~@(bind-parameters bindorder pmeta opts)))))

(defn- execute-insert
  "Executor for definsert.  It prepares, binds, and calls
  executeUpdate.  The return value is based upon getGeneratedKeys."
  [bindorder bindsql pmeta c opts]
  (check-empty "unknown options" (keys opts))
  `(with-open [ps# (.prepareStatement ~c ~bindsql java.sql.Statement/RETURN_GENERATED_KEYS)]
     (if (< 0 (.executeUpdate (doto ps#
                                ~@(bind-parameters bindorder pmeta opts))))
       (with-open [rs# (.getGeneratedKeys ps#)]
         (read-first-row rs#)))))
     

(defn- prepare-and-execute
  "This method recursively iterates through the params list, setting
  up each parameter as needed.  Once params is empty, this method then
  prepares and executes the SQL using the provided executor fn.

  Parameters that require special processing before sending to
  JDBC (e.g., Array and JSON), are bound using (let) blocks.  This
  allows for the same parameter to be used multiple times in the same
  query without multiple setups.  It also, as in the case of Arrays,
  allows for exceptions to be thrown before the PreparedStatement is
  created."
  [params bindorder bindsql pmeta c opts executor]
  (if (empty? params)
    (executor bindorder bindsql pmeta c opts)
    ;; else there is a parameter left to inspect and possibly set up.
    (let [[p & params] params
          ;; remove metadata from the parameter, since we do not want
          ;; it in the parameter setup.
          p (with-meta p nil)]
      
      (cond
        ;; JSON serialization
        (:json (pmeta p))
        `(let [~p (if ~p (cheshire.core/generate-string ~p))]
           ~(prepare-and-execute params bindorder bindsql (assoc-in pmeta [p :tag] java.lang.String) c opts executor))

        ;; SQL Arrays
        (:array (pmeta p))
        `(let [~p (if ~p (.createArrayOf ~c ~(:array (pmeta p)) (object-array ~p)))]
           ~(prepare-and-execute params bindorder bindsql (assoc-in pmeta [p :tag] java.sql.Array) c opts executor))

        :else
        (prepare-and-execute params bindorder bindsql pmeta c opts executor)))))

;; row mapping options
;; full-control -> (my-process-result-set rs#)
;; rs->row -> (loop [ ... ] ... (recur (conj! rows (my-read-row rs#))))
;; 

;; TODO: prepareStatement options (concurrency, holdability, etc...)

(defn- fix-meta-tags
  "Replaces defsql's metadata tag extensions with proper ones."
  [m]
  (if-let [tag (get {'keyword 'clojure.lang.Keyword
                     'symbol 'clojure.lang.Symbol}
                    (:tag m))]
    (assoc m :tag tag)
    m))

(defn- defsql*
  "Workhorse of defsql macros, converts a SQL spec + an executor into
  a function."
  {:arglists '([name docstring? attr-map? [params*] options? sql])}
  [name decl executor]
  (check-arg (symbol? name) "name must be a symbol")
  (let [fmeta (if (string? (first decl))
                {:doc (first decl)}
                {})
        decl (if (string? (first decl))
               (next decl)
               decl)
        fmeta (if (map? (first decl))
                (conj fmeta (first decl))
                fmeta)
        decl (if (map? (first decl))
               (next decl)
               decl)
        check-args false ;; TODO: make this settable
        params (first decl)
        decl (next decl)
        opts (if (map? (first decl))
               (first decl))
        decl (if (map? (first decl))
               (next decl)
               decl)
        sql (first decl)
        fmeta (conj (or (meta name) {}) fmeta)]
    (check-arg (vector? params) "parameters must be a vector")
    (check-arg (string? sql) "SQL must be a string")
    (check-empty "too many arguments" (next decl))

    (let [[bindorder varset bindsql] (map-sql-params *ns* sql)
          bound-set (into #{} bindorder)
          pmeta (merge (zipmap params (map meta params))
                       (zipmap varset (map #(meta (resolve %)) varset)))
          c `c#]
      (check-empty "unused parameter (if intentional, add ^:unused before parameter)"
                   (remove #(or (bound-set %) (:unused (meta %))) params))
      (check-empty "used parameter marked as ^:unused"
                   (filter #(and (bound-set %) (:unused (meta %))) params))
      ;; Our function signature always starts with [^java.sql.Connection
      ;; c] followed by the declared parameters from the user.  To avoid
      ;; incompatibilities, we remove any metadata that is specific to
      ;; defsql (:array and :unused).
      
      ;; TODO: replace :array/:unused/etc... with namespace qualified
      ;; keywords for them.

      ;; Add ::bindorder and ::bindsql as metadata to the function to
      ;; enable testing of the SQL, or other creative ideas.  Note
      ;; that these keywords are qualified to this namespace to avoid
      ;; conflicts.
      `(defn ~(with-meta name (assoc fmeta
                                     ::bindorder (into [] (map str bindorder))
                                     ::bindsql bindsql))
         ~(into [(with-meta c {:tag `java.sql.Connection})]
                (->> params
                     (map #(vary-meta % dissoc :unused)) ;; :array :json
                     (map #(vary-meta % fix-meta-tags))))
         ~@(when check-args ;; TODO: make this a :pre condition
            `(if-not (instance? java.sql.Connection ~c)
               (throw (IllegalArgumentException. (str "invalid connection: " (pr-str ~c))))))
         ~(prepare-and-execute params bindorder bindsql pmeta c opts executor)))))


(defmacro defquery
  "Creates a function that performs a SQL query via JDBC.  The `name'
  parameter is the name of the resulting function.  The `params' is an
  argument list for the function, with the JDBC connection parameter
  automatically inserted at the beginning.  The `sql' parameter is a
  SQL string that will be used to create the prepared statement.
  Occurances of keywords in the SQL string are replaced by
  PreparedStatement binds to the associated parameter in the parameter
  list.  For example:

    (defquery select-users-by-country [country]
       \"SELECT * FROM user WHERE user_country = :country\")

  is expanded to the equivalent to the following:

    (defn select-users-by-country [conn country]
      (with-open [ps (. conn (prepareStatement
                       \"SELECT * FROM user WHERE user_country = ?\"))
                  rs (. (doto ps (.setObject 1 country))
                        (executeQuery))]
         (read-rows rs)))

  Tagging parameters will cause expansion to use more specific
  PreparedStatement binding methods.  If the above example had
  tagged [^String country], then the expansion would use
  (.setString 1 country).

  For primitives, tagging with the box class (e.g., [^Long id]) will
  allow nulls, whereas tagging with the primitive (e.g., [^long id])
  will throw an exception on null.  The primitive (non-nullable)
  version is marginally faster, and can allow early detection of
  errors when nulls are not allowed.

  Adding ^:json to a parameter will cause defquery to serialized the
  parameter to a JSON string, then use .setString to bind it.

  Adding ^{:array \"VARCHAR\"} to a parameter will cause defquery to
  convert the argument (via object-array) to a SQL Array (of type
  'VARCHAR') and bind it using (.setArray).

  Queries may also contain references to dynamic variables, provided
  the dynamic variable is named according to the *varname* convention.
  Both fully-qualified (my-ns/*varname*) and unqualified (*varname*)
  dynamic variables are supported.  However due to a limitation,
  binding based upon metadata is currently only supported for
  fully-qualified dynamic variables.  Thus, with

     (def ^:dynamic ^java.sql.Timestamp *now*)

     \"SELECT ... *now*\"
     will bind with (.setObject *now*)

     \"SELECT ... my-ns/*now*\"
     will bind with (.setTimestamp my-ns/*now*)

  Use the optional options map to change standard behaviors.

  The {:row rowfn} option, where `rowfn' is a function that takes a
  ResultSet can be used to specify a row reader function (useful in
  combination with defrow).

  The {:rows :first} option returns the first row from the
  ResultSet (may be used in combination with {:row rowfn}) instead of
  the usual vector of rows.  If there are no results, nil is returned.

  When {:rows :single} is present, the ResultSet is expected to have
  exactly 1 result.  An exception will be thrown if 0 or 2+ results
  are returned.  Otherwise the single row is returned."
  [name & decl]
  (defsql* name decl execute-query))

(defmacro defupdate
  "Like defquery, but uses .executeUpdate to execute the statement.
  The return value is the result of the executeUpdate (that is, an
  integer).  Does not support :row and :rows options."
  [name & decl]
  (defsql* name decl execute-update))

;; TODO: (definsert insert-name table-name [params ... ])
;; Detect and convert to a SQL string.
;;       -> (definsert insert-name "INSERT INTO table-name (params, ...) VALUES (?, ...)")
(defmacro definsert
  "Like defquery, but uses .executeUpdate to execute the statement.
  The PreparedStatement is created with RETURN_GENERATED_KEYS, and the
  return value is retrieved from the PreparedStatement's
  .getGeneratedKeys result set."
  ;; TODO: needs testing!
  [name & decl]
  (defsql* name decl execute-insert))

(defmacro defquery-
  "Same as defquery, yielding non-public def"
  [name & decls]
  (list* `defquery (vary-meta name assoc :private true) decls))

(defmacro defupdate-
  "Same as defupdate, yielding non-public def"
  [name & decls]
  (list* `defupdate (vary-meta name assoc :private true) decls))

(defmacro definsert-
  "Same as definsert, yielding non-public def"
  [name & decls]
  (list* `definsert (vary-meta name assoc :private true) decls))

(defn- clean-field-metadata
  "Replaces metadata tag extensions (^keyword, ^symbol) and removes
  non-standard metadata from a record field declaration."
  [m]
  (-> (if-let [tag (get extension-tag->class (:tag m))]
        (assoc m :tag tag) m)
      (dissoc :json)))

(defmacro defrow
  "Creates a defrecord and a ResultSet to record function with the
  name rs->RecordName.  The fields may be declared with tags and
  deserialization information to define how the ResultSet will be
  converted to a row.  For example,

    (defrow MyUser [^Long user-id ^keyword state ^:json data])

  Results in a MyUser defrecord, and a function similar to the
  following:

    (defn rs->MyUser [^ResultSet rs]
      (new MyUser
           (.getLong rs 1)
           (keyword (.getString rs 2))
           (cheshire.core/parse-string (.getString rs 3) 3)))

  (Except that SQL NULLs will result in nil instead of
  NullPointerExceptions like would happen in this example.)"
  [nom fields & opts+specs]
  (let [fn-name (symbol (str "rs->" nom))
        ns-part (namespace-munge *ns*)
        classname (symbol (str ns-part "." nom))
        docstring (str "Factory to create " classname " from a java.sql.ResultSet")
        rs `rs#]
  `(do
     (defrecord ~nom ~(into [] (map #(vary-meta % clean-field-metadata) fields)) ~@opts+specs)
     (defn ~(symbol (str "rs->" nom))
       ~docstring
       [~(with-meta rs {:tag java.sql.ResultSet})]
       ;; use the constructor instead of the positional
       ;; factory (->classname) since the positional factory is just a
       ;; wrapper for the constructor.  Also clojure functions have a 20
       ;; argument limit which requires special handling if the limit is
       ;; exceeded, whereas the
       ;; constructor does not.
       (new ~classname
            ~@(for [i (range 0 (count fields))
                    :let [m (meta (fields i))]]
                (cond
                  (:json m)
                  `(if-let [s# (~'.getString ~rs ~(inc i))]
                     (cheshire.core/parse-string s# true)) ;; TODO: make 'true' also configurable

                  :else
                  (get-field (tag-to-class (:tag m)) rs (inc i)))))))))

;; TODO: if tagged function uses primitives and has 4+ arguments, we
;; get the following: "fns taking primitives support only 4 or fewer
;; args, compiling"
