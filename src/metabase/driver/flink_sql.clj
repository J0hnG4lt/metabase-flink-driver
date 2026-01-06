(ns metabase.driver.flink-sql
  "Metabase driver for Apache Flink SQL Gateway via JDBC."
  (:require
   [clojure.java.jdbc :as jdbc]
   [clojure.string :as str]
   [metabase.driver :as driver]
   [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
   [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
   [metabase.driver.sql-jdbc.sync :as sql-jdbc.sync]
   [metabase.driver.sql.query-processor :as sql.qp]
   [metabase.util.log :as log])
  (:import
   [java.io PrintWriter]
   [java.sql Connection DriverManager ResultSet ResultSetMetaData Statement Types]
   [java.time LocalDate LocalDateTime LocalTime OffsetDateTime ZonedDateTime]
   [java.util.logging Logger]
   [javax.sql DataSource]))

(set! *warn-on-reflection* true)

;; ----------------------------------------
;; JDBC Driver Loading
;; ----------------------------------------

;; Ensure Flink JDBC driver is loaded - must be called before DriverManager.getConnection()
;; This is defined early because it's needed by can-connect? and other methods
(defonce ^:private flink-driver-loaded
  (try
    (Class/forName "org.apache.flink.table.jdbc.FlinkDriver")
    (log/info "Flink JDBC driver loaded successfully")
    true
    (catch ClassNotFoundException e
      (log/error e "Failed to load Flink JDBC driver - connection will fail")
      false)))

(defn- ensure-flink-driver!
  "Ensure Flink JDBC driver is loaded. Throws if not available."
  []
  (when-not flink-driver-loaded
    (throw (Exception. "Flink JDBC driver not loaded - check plugin JAR contains Flink dependencies"))))

(defn- get-flink-connection
  "Get a Flink JDBC connection after ensuring the driver is loaded."
  ^Connection [^String jdbc-url]
  (ensure-flink-driver!)
  (DriverManager/getConnection jdbc-url))

;; ----------------------------------------
;; Simple DataSource (bypasses c3p0 pooling)
;; ----------------------------------------

(defn- make-flink-datasource
  "Create a simple non-pooled DataSource for Flink JDBC.
   Flink JDBC doesn't support many standard JDBC operations like clearWarnings,
   which causes issues with c3p0 connection pooling."
  ^DataSource [jdbc-url]
  (reify DataSource
    (getConnection [_]
      (get-flink-connection jdbc-url))
    (getConnection [_ _user _password]
      (get-flink-connection jdbc-url))
    (getLoginTimeout [_] 0)
    (setLoginTimeout [_ _seconds])
    (getLogWriter [_] nil)
    (setLogWriter [_ _out])
    (getParentLogger [_]
      (Logger/getLogger Logger/GLOBAL_LOGGER_NAME))
    (^boolean isWrapperFor [_ ^Class _iface] false)
    (^Object unwrap [_ ^Class _iface] nil)))

;; ----------------------------------------
;; Driver Registration
;; ----------------------------------------

(driver/register! :flink-sql, :parent #{:sql-jdbc})

;; ----------------------------------------
;; Driver Display Name
;; ----------------------------------------

(defmethod driver/display-name :flink-sql [_]
  "Flink SQL")

;; ----------------------------------------
;; Feature Support
;; ----------------------------------------

(doseq [[feature supported?]
        {:describe-fields           true  ;; Use custom describe-fields implementation
         :connection-impersonation  false
         :convert-timezone          false
         :test/jvm-timezone-setting false
         :foreign-keys              false
         :nested-fields             false
         :set-timezone              false}]
  (defmethod driver/database-supports? [:flink-sql feature] [_driver _feature _db] supported?))

;; ----------------------------------------
;; Connection Spec
;; ----------------------------------------

(defmethod sql-jdbc.conn/connection-details->spec :flink-sql
  [_driver {:keys [host port catalog database additional-options] :as _details}]
  ;; Note: Flink JDBC URL format is jdbc:flink://host:port
  ;; Catalog and database are NOT supported in the URL path (unlike MySQL/Postgres)
  ;; Instead, we issue USE CATALOG and USE DATABASE statements after connecting
  (let [host           (or host "localhost")
        port           (or port 8083)
        query-string   (when-not (str/blank? additional-options)
                         (str "?" additional-options))
        jdbc-url       (str "jdbc:flink://" host ":" port (or query-string ""))]
    (log/debug "Flink SQL JDBC URL:" jdbc-url)
    (log/debug "Flink catalog:" catalog "database:" database "(set via USE statements)")
    {:classname   "org.apache.flink.table.jdbc.FlinkDriver"
     :subprotocol "flink"
     :subname     (str "//" host ":" port (or query-string ""))
     ;; Store catalog/database for use in initialize-session!
     :flink-catalog catalog
     :flink-database database}))

;; Helper to extract catalog/database from various input types
(defn- get-catalog-database
  "Extract catalog and database settings from database spec or details.
   Returns {:catalog \"...\" :database \"...\"} or empty map if not set."
  [db-or-id-or-spec]
  (cond
    ;; Connection spec with flink-catalog/flink-database keys
    (and (map? db-or-id-or-spec) (:flink-catalog db-or-id-or-spec))
    {:catalog  (:flink-catalog db-or-id-or-spec)
     :database (:flink-database db-or-id-or-spec)}

    ;; Database map with details
    (and (map? db-or-id-or-spec) (:details db-or-id-or-spec))
    (let [details (:details db-or-id-or-spec)]
      {:catalog  (:catalog details)
       :database (:database details)})

    ;; Integer ID - lookup from Metabase DB
    (integer? db-or-id-or-spec)
    (try
      (require 'metabase.lib.metadata.jvm)
      (let [metadata-provider ((resolve 'metabase.lib.metadata.jvm/application-database-metadata-provider) db-or-id-or-spec)
            db-info           ((resolve 'metabase.lib.metadata.protocols/database) metadata-provider)
            details           (:details db-info)]
        {:catalog  (:catalog details)
         :database (:database details)})
      (catch Exception e
        (log/debug "Could not get catalog/database for DB ID" db-or-id-or-spec ":" (.getMessage e))
        {}))

    :else {}))

;; Helper to extract JDBC URL from database spec (used throughout driver)
(defn- get-jdbc-url-from-db
  "Extract JDBC URL from database or spec. Works with both database maps and IDs."
  [db-or-id-or-spec]
  (cond
    ;; Handle connection spec with subname directly (e.g. {:subprotocol "flink" :subname "//host:port"})
    (and (map? db-or-id-or-spec) (:subname db-or-id-or-spec))
    (let [subname (:subname db-or-id-or-spec)]
      (log/info "Building Flink JDBC URL from spec subname:" subname)
      (str "jdbc:flink:" subname))

    ;; Handle datasource key (pooled connection spec from c3p0)
    ;; The datasource contains the actual JDBC connection info but we can't easily extract it
    ;; Instead, try to get database ID and look it up, or extract from classname/subprotocol
    (and (map? db-or-id-or-spec) (:datasource db-or-id-or-spec))
    (let [db-id (or (:id db-or-id-or-spec)
                    (:db_id db-or-id-or-spec)
                    (get-in db-or-id-or-spec [:database :id]))]
      (log/info "Building Flink JDBC URL from datasource, DB ID:" db-id "keys:" (keys db-or-id-or-spec))
      (if db-id
        (recur db-id)
        ;; If we can't get the ID, check for subname in the spec
        (if-let [subname (:subname db-or-id-or-spec)]
          (do
            (log/info "Using subname from datasource spec:" subname)
            (str "jdbc:flink:" subname))
          ;; Last resort - construct from any available details
          (let [details (or (:details db-or-id-or-spec) {})
                host    (or (:host details) "sql-gateway")  ;; Default to sql-gateway for Docker
                port    (or (:port details) 8083)]
            (log/info "Fallback to default host:" host)
            (str "jdbc:flink://" host ":" port)))))

    :else
    ;; Otherwise resolve database details
    (let [;; Resolve database from ID if needed
          db      (cond
                    ;; Already a map with details key
                    (and (map? db-or-id-or-spec) (:details db-or-id-or-spec))
                    db-or-id-or-spec

                    ;; Map with host key - likely details map directly
                    (and (map? db-or-id-or-spec) (:host db-or-id-or-spec))
                    {:details db-or-id-or-spec}

                    ;; Integer ID - lookup from Metabase DB
                    (integer? db-or-id-or-spec)
                    (try
                      (require 'metabase.lib.metadata.jvm)
                      (let [metadata-provider ((resolve 'metabase.lib.metadata.jvm/application-database-metadata-provider) db-or-id-or-spec)
                            db-info           ((resolve 'metabase.lib.metadata.protocols/database) metadata-provider)]
                        db-info)
                      (catch Exception _
                        ;; Fallback: try direct database lookup
                        (try
                          (require 'metabase.models.database)
                          ((resolve 'metabase.models.database/select-one) db-or-id-or-spec)
                          (catch Exception e
                            (log/warn "Failed to lookup database by ID:" db-or-id-or-spec e)
                            nil))))

                    ;; Unknown map - log its keys for debugging
                    (map? db-or-id-or-spec)
                    (do
                      (log/info "Unknown map structure, keys:" (keys db-or-id-or-spec))
                      nil)

                    :else nil)
          ;; Extract details from database record
          details (cond
                    (:details db) (:details db)
                    (:host db) db  ;; Already is details map
                    :else {})
          host    (or (:host details) "localhost")
          port    (or (:port details) 8083)
          ;; Note: catalog/database are NOT included in URL - Flink JDBC doesn't support it
          ;; They are handled via USE statements in initialize-session!
          options (:additional-options details)
          query   (when-not (str/blank? options) (str "?" options))]
      (log/info "Building Flink JDBC URL - host:" host "port:" port "from:" (type db-or-id-or-spec))
      (str "jdbc:flink://" host ":" port (or query "")))))

;; ----------------------------------------
;; Connection Testing
;; ----------------------------------------

(defmethod driver/can-connect? :flink-sql
  [driver details]
  (let [spec (sql-jdbc.conn/connection-details->spec driver details)
        jdbc-url (str "jdbc:" (:subprotocol spec) ":" (:subname spec))]
    (try
      ;; Flink JDBC doesn't support prepareStatement
      ;; Use execute() + getResultSet() for catalog operations which are fast
      (with-open [conn (get-flink-connection jdbc-url)]
        (with-open [stmt (.createStatement conn)]
          (let [has-rs (.execute stmt "SHOW CATALOGS")]
            (if has-rs
              (with-open [rs (.getResultSet stmt)]
                (.next rs)
                true)
              ;; If no result set, connection still worked
              true))))
      (catch Exception e
        (log/warn e "Flink SQL connection test failed")
        false))))

;; ----------------------------------------
;; Error Message Handling
;; ----------------------------------------

(defmethod driver/humanize-connection-error-message :flink-sql
  [_driver message]
  (condp re-find message
    #"Connection refused"
    "Connection refused. Make sure the Flink SQL Gateway is running and the host/port are correct."

    #"UnknownHostException"
    "Unknown host. Please check the hostname."

    #"No suitable driver"
    "No suitable JDBC driver found. Make sure the Flink JDBC driver is properly installed."

    #"timeout"
    "Connection timed out. The Flink SQL Gateway may be slow to respond or unreachable."

    message))

;; ----------------------------------------
;; Database Type Mapping
;; ----------------------------------------

(defmethod sql-jdbc.sync/database-type->base-type :flink-sql
  [_driver database-type]
  (condp re-matches (str/upper-case (name database-type))
    #"TINYINT"                 :type/Integer
    #"SMALLINT"                :type/Integer
    #"INT"                     :type/Integer
    #"INTEGER"                 :type/Integer
    #"BIGINT"                  :type/BigInteger
    #"FLOAT"                   :type/Float
    #"DOUBLE"                  :type/Float
    #"DOUBLE PRECISION"        :type/Float
    #"DECIMAL.*"               :type/Decimal
    #"NUMERIC.*"               :type/Decimal
    #"CHAR.*"                  :type/Text
    #"VARCHAR.*"               :type/Text
    #"STRING"                  :type/Text
    #"BINARY.*"                :type/Text
    #"VARBINARY.*"             :type/Text
    #"BYTES"                   :type/Text
    #"BOOLEAN"                 :type/Boolean
    #"DATE"                    :type/Date
    #"TIME.*"                  :type/Time
    #"TIMESTAMP.*"             :type/DateTime
    #"TIMESTAMP_LTZ.*"         :type/DateTimeWithLocalTZ
    #"ARRAY.*"                 :type/Array
    #"MAP.*"                   :type/Structured
    #"MULTISET.*"              :type/Structured
    #"ROW.*"                   :type/Structured
    :type/*))

;; ----------------------------------------
;; Schema Synchronization
;; ----------------------------------------

(defmethod sql-jdbc.sync/active-tables :flink-sql
  [& args]
  (apply (get-method sql-jdbc.sync/active-tables :sql-jdbc) args))

(declare initialize-session!)

(defmethod driver/describe-database :flink-sql
  [_driver database]
  (let [jdbc-url (get-jdbc-url-from-db database)
        cat-db   (get-catalog-database database)
        catalog  (:catalog cat-db)
        db-name  (:database cat-db)]
    (try
      (with-open [conn (get-flink-connection jdbc-url)]
        ;; Initialize session with tables before describing (with catalog/database)
        (initialize-session! conn catalog db-name)
        (with-open [stmt (.createStatement conn)]
          (let [has-rs (.execute stmt "SHOW TABLES")]
            (if has-rs
              (with-open [rs (.getResultSet stmt)]
                (let [tables (loop [results []]
                               (if (.next rs)
                                 (recur (conj results {:name   (.getString rs 1)
                                                       :schema nil}))
                                 results))]
                  {:tables (set tables)}))
              {:tables #{}}))))
      (catch Exception e
        (log/warn e "Failed to describe database using SHOW TABLES")
        {:tables #{}}))))

(defmethod driver/describe-table :flink-sql
  [driver database table]
  (let [jdbc-url   (get-jdbc-url-from-db database)
        cat-db     (get-catalog-database database)
        catalog    (:catalog cat-db)
        db-name    (:database cat-db)
        table-name (:name table)]
    (try
      (with-open [conn (get-flink-connection jdbc-url)]
        ;; Initialize session with tables before describing (with catalog/database)
        (initialize-session! conn catalog db-name)
        (with-open [stmt (.createStatement conn)]
          (let [has-rs (.execute stmt (str "DESCRIBE `" table-name "`"))]
            (if has-rs
              (with-open [rs (.getResultSet stmt)]
                (let [columns (loop [idx 0, results []]
                                (if (.next rs)
                                  (let [col-name (.getString rs 1)
                                        col-type (.getString rs 2)]
                                    (recur (inc idx)
                                           (conj results
                                                 {:name              col-name
                                                  :database-type     col-type
                                                  :base-type         (sql-jdbc.sync/database-type->base-type driver col-type)
                                                  :database-position idx})))
                                  results))]
                  {:name   table-name
                   :schema (:schema table)
                   :fields (set columns)}))
              {:name table-name :schema (:schema table) :fields #{}}))))
      (catch Exception e
        (log/warn e "Failed to describe table using DESCRIBE")
        {:name table-name :schema (:schema table) :fields #{}}))))

;; Foreign keys not supported in Flink
(defmethod driver/describe-table-fks :flink-sql
  [_driver _database _table]
  #{})

;; ----------------------------------------
;; Describe Fields (for field sync)
;; ----------------------------------------

(defmethod driver/describe-fields :flink-sql
  [driver database & {:keys [table-names]}]
  ;; Returns a reducible of field metadata for all tables (or specified tables)
  (let [jdbc-url (get-jdbc-url-from-db database)
        cat-db   (get-catalog-database database)
        catalog  (:catalog cat-db)
        db-name  (:database cat-db)]
    (reify clojure.lang.IReduceInit
      (reduce [_ rf init]
        (try
          (with-open [conn (get-flink-connection jdbc-url)]
            ;; Initialize session with tables (with catalog/database)
            (initialize-session! conn catalog db-name)
            ;; Get list of tables to describe
            (let [tables-to-describe (if (seq table-names)
                                       table-names
                                       ;; Get all tables via SHOW TABLES
                                       (with-open [stmt (.createStatement conn)]
                                         (let [has-rs (.execute stmt "SHOW TABLES")]
                                           (if has-rs
                                             (with-open [rs (.getResultSet stmt)]
                                               (loop [tables []]
                                                 (if (.next rs)
                                                   (recur (conj tables (.getString rs 1)))
                                                   tables)))
                                             []))))]
              ;; For each table, get its columns
              (reduce
               (fn [acc table-name]
                 (try
                   (with-open [stmt (.createStatement conn)]
                     (let [has-rs (.execute stmt (str "DESCRIBE `" table-name "`"))]
                       (if has-rs
                         (with-open [rs (.getResultSet stmt)]
                           (loop [acc acc, idx 0]
                             (if (.next rs)
                               (let [col-name (.getString rs 1)
                                     col-type (.getString rs 2)
                                     ;; Column 3 is nullable (Boolean in Flink)
                                     nullable (try (.getBoolean rs 3) (catch Exception _ true))
                                     field-meta {:table-name     table-name
                                                 :name           col-name
                                                 :database-type  col-type
                                                 :base-type      (sql-jdbc.sync/database-type->base-type driver col-type)
                                                 :database-position idx
                                                 :database-required (not nullable)
                                                 :database-is-auto-increment false}]
                                 (recur (rf acc field-meta) (inc idx)))
                               acc)))
                         acc)))
                   (catch Exception e
                     (log/warn e (str "Failed to describe table: " table-name))
                     acc)))
               init
               tables-to-describe)))
          (catch Exception e
            (log/warn e "Failed in describe-fields")
            init))))))

;; ----------------------------------------
;; SQL Generation
;; ----------------------------------------

;; Flink SQL uses backticks for identifiers
(defmethod sql.qp/quote-style :flink-sql [_driver] :mysql)

;; ----------------------------------------
;; Result Set Handling
;; ----------------------------------------

(defmethod sql-jdbc.execute/read-column-thunk [:flink-sql Types/TIMESTAMP]
  [_driver ^ResultSet rs _rsmeta ^Integer i]
  (fn []
    (when-let [t (.getTimestamp rs i)]
      (.toLocalDateTime t))))

(defmethod sql-jdbc.execute/read-column-thunk [:flink-sql Types/DATE]
  [_driver ^ResultSet rs _rsmeta ^Integer i]
  (fn []
    (when-let [d (.getDate rs i)]
      (.toLocalDate d))))

(defmethod sql-jdbc.execute/read-column-thunk [:flink-sql Types/TIME]
  [_driver ^ResultSet rs _rsmeta ^Integer i]
  (fn []
    (when-let [t (.getTime rs i)]
      (.toLocalTime t))))

;; ----------------------------------------
;; Parameter Binding
;; ----------------------------------------

(defmethod sql-jdbc.execute/set-parameter [:flink-sql LocalDate]
  [_driver ^java.sql.PreparedStatement ps ^Integer i t]
  (.setDate ps i (java.sql.Date/valueOf t)))

(defmethod sql-jdbc.execute/set-parameter [:flink-sql LocalDateTime]
  [_driver ^java.sql.PreparedStatement ps ^Integer i t]
  (.setTimestamp ps i (java.sql.Timestamp/valueOf t)))

(defmethod sql-jdbc.execute/set-parameter [:flink-sql LocalTime]
  [_driver ^java.sql.PreparedStatement ps ^Integer i t]
  (.setTime ps i (java.sql.Time/valueOf t)))

(defmethod sql-jdbc.execute/set-parameter [:flink-sql OffsetDateTime]
  [_driver ^java.sql.PreparedStatement ps ^Integer i t]
  (.setTimestamp ps i (java.sql.Timestamp/from (.toInstant t))))

(defmethod sql-jdbc.execute/set-parameter [:flink-sql ZonedDateTime]
  [_driver ^java.sql.PreparedStatement ps ^Integer i t]
  (.setTimestamp ps i (java.sql.Timestamp/from (.toInstant t))))

;; ----------------------------------------
;; Sync Filtering
;; ----------------------------------------

(defmethod sql-jdbc.sync/excluded-schemas :flink-sql
  [_driver]
  #{"information_schema" "INFORMATION_SCHEMA"})

;; ----------------------------------------
;; Session Initialization SQL
;; ----------------------------------------

;; Default SQL to create tables in each session (for testing)
;; These tables use the datagen connector with bounded rows for batch queries.
;;
;; IMPORTANT: Tables MUST have 'number-of-rows' set for JDBC queries to work.
;; Without it, the table becomes an unbounded stream and queries will hang forever.
;; This is a Flink JDBC limitation (FLIP-293: JDBC only supports batch mode).
;;
;; Table Types:
;; - BOUNDED (queryable): datagen with 'number-of-rows', filesystem, JDBC sources
;; - UNBOUNDED (hangs): datagen without row limit, Kafka without bounded mode
;; - STREAMING WITH BOUNDS: Kafka with 'scan.bounded.mode' = 'latest-offset'
(def ^:private default-init-sql
  [;; =====================================================
   ;; BOUNDED TABLES (10K+ rows) - Full SQL support
   ;; =====================================================

   ;; Users table - 10,000 rows for testing large result sets
   "CREATE TABLE IF NOT EXISTS users (
      user_id INT,
      username STRING,
      email STRING,
      created_at TIMESTAMP(3),
      age INT,
      country STRING
    ) WITH (
      'connector' = 'datagen',
      'number-of-rows' = '10000',
      'fields.user_id.kind' = 'sequence',
      'fields.user_id.start' = '1',
      'fields.user_id.end' = '10000',
      'fields.username.length' = '10',
      'fields.email.length' = '15',
      'fields.age.min' = '18',
      'fields.age.max' = '80',
      'fields.country.length' = '5'
    )"

   ;; Orders table - 50,000 rows for JOIN and aggregation testing
   "CREATE TABLE IF NOT EXISTS orders (
      order_id INT,
      user_id INT,
      product_name STRING,
      quantity INT,
      unit_price DECIMAL(10, 2),
      order_time TIMESTAMP(3),
      status STRING
    ) WITH (
      'connector' = 'datagen',
      'number-of-rows' = '50000',
      'fields.order_id.kind' = 'sequence',
      'fields.order_id.start' = '1',
      'fields.order_id.end' = '50000',
      'fields.user_id.min' = '1',
      'fields.user_id.max' = '10000',
      'fields.product_name.length' = '12',
      'fields.quantity.min' = '1',
      'fields.quantity.max' = '10',
      'fields.unit_price.min' = '1',
      'fields.unit_price.max' = '500',
      'fields.status.length' = '8'
    )"

   ;; Products table - 1,000 rows for product catalog
   "CREATE TABLE IF NOT EXISTS products (
      product_id INT,
      product_name STRING,
      category STRING,
      price DECIMAL(10, 2),
      stock_quantity INT,
      last_updated TIMESTAMP(3)
    ) WITH (
      'connector' = 'datagen',
      'number-of-rows' = '1000',
      'fields.product_id.kind' = 'sequence',
      'fields.product_id.start' = '1',
      'fields.product_id.end' = '1000',
      'fields.product_name.length' = '15',
      'fields.category.length' = '8',
      'fields.price.min' = '5',
      'fields.price.max' = '1000',
      'fields.stock_quantity.min' = '0',
      'fields.stock_quantity.max' = '500'
    )"

   ;; Page views table - 100,000 rows for analytics testing
   "CREATE TABLE IF NOT EXISTS page_views (
      view_id BIGINT,
      user_id INT,
      page_url STRING,
      referrer STRING,
      view_time TIMESTAMP(3),
      session_id STRING,
      device_type STRING
    ) WITH (
      'connector' = 'datagen',
      'number-of-rows' = '100000',
      'fields.view_id.kind' = 'sequence',
      'fields.view_id.start' = '1',
      'fields.view_id.end' = '100000',
      'fields.user_id.min' = '1',
      'fields.user_id.max' = '10000',
      'fields.page_url.length' = '20',
      'fields.referrer.length' = '15',
      'fields.session_id.length' = '32',
      'fields.device_type.length' = '6'
    )"

   ;; =====================================================
   ;; UNBOUNDED STREAMING TABLE - Queries will HANG!
   ;; This demonstrates the Flink JDBC batch-mode limitation.
   ;; DO NOT query this table - it's here to show what NOT to do.
   ;; =====================================================

   ;; Streaming events - NO number-of-rows = UNBOUNDED STREAM
   ;; WARNING: Any query on this table will hang forever!
   "CREATE TABLE IF NOT EXISTS streaming_events (
      event_id INT,
      event_type STRING,
      event_data STRING,
      event_time TIMESTAMP(3),
      WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
    ) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '10',
      'fields.event_id.kind' = 'random',
      'fields.event_id.min' = '1',
      'fields.event_id.max' = '1000000',
      'fields.event_type.length' = '10',
      'fields.event_data.length' = '50'
    )"])

(defn- initialize-session!
  "Execute initialization SQL to create tables in this session.
   If catalog and database are provided, switches to them first via USE statements."
  ([^Connection conn]
   (initialize-session! conn nil nil))
  ([^Connection conn catalog database]
   (log/info "Initializing Flink session..." (when catalog (str "catalog=" catalog)) (when database (str "database=" database)))
   (with-open [stmt (.createStatement conn)]
     ;; Set catalog/database if provided
     (when (and catalog (not (str/blank? catalog)))
       (try
         (let [use-catalog (str "USE CATALOG `" catalog "`")]
           (log/info "Setting catalog:" use-catalog)
           (.execute stmt use-catalog))
         (catch Exception e
           (log/warn "Failed to set catalog" catalog ":" (.getMessage e)))))
     (when (and database (not (str/blank? database)))
       (try
         (let [use-db (str "USE `" database "`")]
           (log/info "Setting database:" use-db)
           (.execute stmt use-db))
         (catch Exception e
           (log/warn "Failed to set database" database ":" (.getMessage e)))))
     ;; Create default tables
     (doseq [sql default-init-sql]
       (try
         (.execute stmt sql)
         (log/debug "Executed init SQL successfully")
         (catch Exception e
           (log/debug "Init SQL execution (may be expected if table exists):" (.getMessage e))))))))

;; ----------------------------------------
;; Connection Workarounds
;; ----------------------------------------

;; Flink JDBC doesn't support many standard JDBC operations (clearWarnings, prepareStatement, etc.)
;; that c3p0 connection pooling requires. We bypass the pool entirely and create connections directly.

(defmethod sql-jdbc.execute/do-with-connection-with-options :flink-sql
  [_driver db-or-id-or-spec _options f]
  ;; Bypass c3p0 pooling - Flink JDBC doesn't support clearWarnings() and other operations
  (let [jdbc-url    (get-jdbc-url-from-db db-or-id-or-spec)
        cat-db      (get-catalog-database db-or-id-or-spec)
        catalog     (:catalog cat-db)
        database    (:database cat-db)]
    (log/debug "Opening direct Flink connection to:" jdbc-url "catalog:" catalog "database:" database)
    (with-open [conn (get-flink-connection jdbc-url)]
      (try
        (.setAutoCommit conn true)
        (catch Exception _))
      ;; Initialize the session with default tables (and set catalog/database if specified)
      (initialize-session! conn catalog database)
      (f conn))))

;; NOTE: describe-database, describe-table, and describe-fields also use get-flink-connection
;; They are defined earlier in the file but will use the get-flink-connection defined at top

;; ----------------------------------------
;; Statement Creation Workaround
;; ----------------------------------------

;; Flink JDBC doesn't support many standard Statement operations like setMaxRows, setFetchSize, etc.
;; Create a wrapper that delegates all calls but ignores unsupported operations.

(defn- wrap-flink-statement
  "Wrap a Flink Statement to handle unsupported operations gracefully."
  ^Statement [^Statement stmt]
  (proxy [Statement] []
    ;; Query execution methods - delegate to underlying statement
    (execute [sql] (.execute stmt sql))
    (executeQuery [sql] (.executeQuery stmt sql))
    (executeUpdate [sql] (.executeUpdate stmt sql))
    (getResultSet [] (.getResultSet stmt))
    (getUpdateCount [] (.getUpdateCount stmt))
    (getMoreResults [] (.getMoreResults stmt))
    (getWarnings [] nil)  ;; clearWarnings not supported, return nil
    (clearWarnings [])    ;; no-op
    (close [] (.close stmt))
    (isClosed [] (.isClosed stmt))

    ;; Unsupported operations - no-op or return safe defaults
    (setMaxRows [_max])        ;; no-op - not supported
    (setFetchSize [_size])     ;; no-op - not supported
    (setQueryTimeout [_secs])  ;; no-op - not supported
    (setFetchDirection [_dir]) ;; no-op - not supported
    (setEscapeProcessing [_b]) ;; no-op - not supported
    (setCursorName [_name])    ;; no-op - not supported

    ;; Getters return safe defaults
    (getMaxRows [] 0)
    (getFetchSize [] 0)
    (getQueryTimeout [] 0)
    (getFetchDirection [] ResultSet/FETCH_FORWARD)
    (getResultSetType [] ResultSet/TYPE_FORWARD_ONLY)
    (getResultSetConcurrency [] ResultSet/CONCUR_READ_ONLY)
    (getResultSetHoldability [] ResultSet/CLOSE_CURSORS_AT_COMMIT)
    (getConnection [] (.getConnection stmt))

    ;; Batch operations - delegate
    (addBatch [sql] (.addBatch stmt sql))
    (clearBatch [] (.clearBatch stmt))
    (executeBatch [] (.executeBatch stmt))

    ;; Misc
    (cancel [] (.cancel stmt))
    (isPoolable [] false)
    (setPoolable [_p])
    (isCloseOnCompletion [] false)
    (closeOnCompletion [])))

(defmethod sql-jdbc.execute/statement :flink-sql
  [_driver ^Connection conn]
  ;; Flink only supports the no-args version of createStatement()
  ;; Wrap it to handle unsupported operations gracefully
  (wrap-flink-statement (.createStatement conn)))

;; ----------------------------------------
;; DDL Statement Support
;; ----------------------------------------

(defn- strip-sql-comments
  "Remove SQL comments from the beginning of a query.
   Metabase prepends comments like '-- Metabase::...' to native queries."
  [^String sql]
  (when sql
    ;; Remove single-line comments at the start
    (loop [s (str/trim sql)]
      (cond
        ;; Single-line comment: -- ...
        (str/starts-with? s "--")
        (let [newline-idx (str/index-of s "\n")]
          (if newline-idx
            (recur (str/trim (subs s (inc newline-idx))))
            ""))  ;; No newline, entire string is a comment

        ;; Multi-line comment: /* ... */
        (str/starts-with? s "/*")
        (let [end-idx (str/index-of s "*/")]
          (if end-idx
            (recur (str/trim (subs s (+ end-idx 2))))
            ""))  ;; Unclosed comment

        :else s))))

(defn- ddl-statement?
  "Check if SQL is a DDL statement (CREATE, DROP, ALTER, etc.)
   These need special handling as they don't return result sets.
   Handles Metabase comment prefix (-- Metabase::...) by stripping comments first."
  [^String sql]
  (when sql
    (let [;; Strip any leading comments (Metabase adds '-- Metabase::...' prefix)
          clean-sql (strip-sql-comments sql)
          trimmed   (-> clean-sql str/upper-case)]
      (or (str/starts-with? trimmed "CREATE ")
          (str/starts-with? trimmed "DROP ")
          (str/starts-with? trimmed "ALTER ")
          (str/starts-with? trimmed "TRUNCATE ")
          (str/starts-with? trimmed "USE ")
          (str/starts-with? trimmed "SET ")))))

;; Create a mock ResultSetMetaData for DDL responses
(defn- create-ddl-metadata
  "Create a mock ResultSetMetaData that describes a single 'result' column."
  ^ResultSetMetaData []
  (proxy [ResultSetMetaData] []
    (getColumnCount [] 1)
    (getColumnName [col] "result")
    (getColumnLabel [col] "result")
    (getColumnType [col] Types/VARCHAR)
    (getColumnTypeName [col] "VARCHAR")
    (getColumnClassName [col] "java.lang.String")
    (isNullable [col] ResultSetMetaData/columnNoNulls)
    (getPrecision [col] 0)
    (getScale [col] 0)
    (getTableName [col] "")
    (getSchemaName [col] "")
    (getCatalogName [col] "")
    (isAutoIncrement [col] false)
    (isCaseSensitive [col] true)
    (isSearchable [col] true)
    (isCurrency [col] false)
    (isSigned [col] false)
    (isReadOnly [col] true)
    (isWritable [col] false)
    (isDefinitelyWritable [col] false)
    (getColumnDisplaySize [col] 10)))

;; Create a mock ResultSet for DDL responses
;; This returns a single row with "OK" result
;; Using proxy instead of reify to handle overloaded methods
(defn- create-ddl-result-set
  "Create a mock ResultSet that returns a single 'OK' row for DDL statements."
  ^ResultSet []
  (let [row-returned (atom false)
        metadata     (create-ddl-metadata)]
    (proxy [ResultSet] []
      ;; Core navigation methods
      (next []
        (if @row-returned
          false
          (do (reset! row-returned true) true)))
      (close [])
      (isClosed [] false)

      ;; Column value getters - all return "OK" or appropriate defaults
      ;; proxy handles both int and String overloads
      (getString [col] "OK")
      (getObject [col] "OK")
      (getInt [col] 0)
      (getLong [col] 0)
      (getBoolean [col] true)
      (getDouble [col] 0.0)
      (getFloat [col] (float 0.0))
      (getBigDecimal [col] (BigDecimal. 0))
      (getDate [col] nil)
      (getTime [col] nil)
      (getTimestamp [col] nil)
      (getBytes [col] nil)
      (getArray [col] nil)

      ;; wasNull - always false since we have a value
      (wasNull [] false)

      ;; Metadata
      (getMetaData [] metadata))))

;; Override execute-statement! to handle DDL statements
;; For native queries, sql-jdbc calls execute-statement! which expects a ResultSet
;; DDL statements (CREATE, DROP, ALTER) don't return result sets, so we handle them specially
(defmethod sql-jdbc.execute/execute-statement! :flink-sql
  [driver ^Statement stmt ^String sql]
  ;; Strip any Metabase comments to check for DDL and execute clean SQL
  (let [clean-sql (strip-sql-comments sql)]
    (log/info "execute-statement! called with SQL:" (subs clean-sql 0 (min 80 (count clean-sql))))
    (if (ddl-statement? clean-sql)
      ;; DDL statement - execute without expecting a ResultSet
      (do
        (log/info "Executing DDL statement via execute-statement!:" (subs clean-sql 0 (min 100 (count clean-sql))))
        ;; Use .execute() for DDL - it works unlike .executeUpdate() in Flink JDBC
        (.execute stmt clean-sql)
        (log/info "DDL executed successfully")
        ;; Return a mock ResultSet with "OK" result
        (create-ddl-result-set))
      ;; Regular query - use standard execute
      (do
        (log/info "Executing regular query via execute-statement!")
        (.execute stmt clean-sql)
        (.getResultSet stmt)))))
