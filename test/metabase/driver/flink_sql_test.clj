(ns metabase.driver.flink-sql-test
  (:require
   [clojure.test :refer :all]
   [metabase.driver :as driver]
   [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]))

;; ----------------------------------------
;; Driver Registration Tests
;; ----------------------------------------

(deftest driver-registration-test
  (testing "Flink SQL driver is registered"
    (is (contains? (driver/available-drivers) :flink-sql))))

(deftest display-name-test
  (testing "Display name is correct"
    (is (= "Flink SQL" (driver/display-name :flink-sql)))))

;; ----------------------------------------
;; Connection Spec Tests
;; ----------------------------------------

(deftest connection-details->spec-test
  (testing "Basic connection spec"
    (let [spec (sql-jdbc.conn/connection-details->spec
                :flink-sql
                {:host "localhost"
                 :port 8083})]
      (is (= "org.apache.flink.table.jdbc.FlinkDriver" (:classname spec)))
      (is (= "flink" (:subprotocol spec)))
      (is (= "//localhost:8083" (:subname spec)))))

  (testing "Connection spec with catalog and database"
    (let [spec (sql-jdbc.conn/connection-details->spec
                :flink-sql
                {:host     "flink-gateway.example.com"
                 :port     8083
                 :catalog  "my_catalog"
                 :database "my_database"})]
      (is (= "//flink-gateway.example.com:8083/my_catalog/my_database" (:subname spec)))))

  (testing "Connection spec with only catalog"
    (let [spec (sql-jdbc.conn/connection-details->spec
                :flink-sql
                {:host    "localhost"
                 :port    8083
                 :catalog "default_catalog"})]
      (is (= "//localhost:8083/default_catalog" (:subname spec)))))

  (testing "Connection spec with additional options"
    (let [spec (sql-jdbc.conn/connection-details->spec
                :flink-sql
                {:host               "localhost"
                 :port               8083
                 :additional-options "timeout=30000&retries=3"})]
      (is (= "//localhost:8083?timeout=30000&retries=3" (:subname spec)))))

  (testing "Connection spec with defaults"
    (let [spec (sql-jdbc.conn/connection-details->spec
                :flink-sql
                {})]
      (is (= "//localhost:8083" (:subname spec))))))

;; ----------------------------------------
;; Error Message Tests
;; ----------------------------------------

(deftest humanize-connection-error-message-test
  (testing "Connection refused error"
    (is (re-find #"running"
                 (driver/humanize-connection-error-message
                  :flink-sql
                  "java.net.ConnectException: Connection refused"))))

  (testing "Unknown host error"
    (is (re-find #"hostname"
                 (driver/humanize-connection-error-message
                  :flink-sql
                  "java.net.UnknownHostException: unknown-host"))))

  (testing "No suitable driver error"
    (is (re-find #"driver"
                 (driver/humanize-connection-error-message
                  :flink-sql
                  "java.sql.SQLException: No suitable driver"))))

  (testing "Timeout error"
    (is (re-find #"timed out"
                 (driver/humanize-connection-error-message
                  :flink-sql
                  "Connection timeout after 30000ms"))))

  (testing "Unknown error passes through"
    (let [msg "Some unknown error message"]
      (is (= msg (driver/humanize-connection-error-message :flink-sql msg))))))

;; ----------------------------------------
;; Feature Support Tests
;; ----------------------------------------

(deftest feature-support-test
  (testing "Supported features"
    (is (true? (driver/database-supports? :flink-sql :describe-fields nil)))
    (is (true? (driver/database-supports? :flink-sql :parameterized-sql nil))))

  (testing "Unsupported features"
    (is (false? (driver/database-supports? :flink-sql :connection-impersonation nil)))
    (is (false? (driver/database-supports? :flink-sql :foreign-keys nil)))
    (is (false? (driver/database-supports? :flink-sql :convert-timezone nil)))))

;; ----------------------------------------
;; Database Type Mapping Tests
;; ----------------------------------------

(deftest database-type->base-type-test
  (testing "Numeric types"
    (is (= :type/Integer
           (#'metabase.driver.flink-sql/database-type->base-type :flink-sql "INT")))
    (is (= :type/BigInteger
           (#'metabase.driver.flink-sql/database-type->base-type :flink-sql "BIGINT")))
    (is (= :type/Float
           (#'metabase.driver.flink-sql/database-type->base-type :flink-sql "DOUBLE")))
    (is (= :type/Decimal
           (#'metabase.driver.flink-sql/database-type->base-type :flink-sql "DECIMAL(10,2)"))))

  (testing "String types"
    (is (= :type/Text
           (#'metabase.driver.flink-sql/database-type->base-type :flink-sql "VARCHAR(255)")))
    (is (= :type/Text
           (#'metabase.driver.flink-sql/database-type->base-type :flink-sql "STRING"))))

  (testing "Date/time types"
    (is (= :type/Date
           (#'metabase.driver.flink-sql/database-type->base-type :flink-sql "DATE")))
    (is (= :type/DateTime
           (#'metabase.driver.flink-sql/database-type->base-type :flink-sql "TIMESTAMP")))
    (is (= :type/DateTimeWithLocalTZ
           (#'metabase.driver.flink-sql/database-type->base-type :flink-sql "TIMESTAMP_LTZ"))))

  (testing "Complex types"
    (is (= :type/Array
           (#'metabase.driver.flink-sql/database-type->base-type :flink-sql "ARRAY<STRING>")))
    (is (= :type/Structured
           (#'metabase.driver.flink-sql/database-type->base-type :flink-sql "MAP<STRING,INT>")))))
