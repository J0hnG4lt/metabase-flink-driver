(ns build
  (:require [clojure.tools.build.api :as b]
            [clojure.string :as str]))

(def lib 'com.github.j0hng4lt/metabase-flink-driver)
(def version "1.0.0")
(def class-dir "target/classes")

;; Supported Flink versions and their JDBC driver versions
(def flink-versions
  {"1.18" "1.18.1"
   "1.19" "1.19.3"
   "1.20" "1.20.3"
   "2.1"  "2.1.1"})

(def default-flink-version "2.1")

(defn- get-flink-version
  "Get Flink version from opts or use default"
  [opts]
  (or (:flink-version opts) default-flink-version))

(defn- uber-file-name
  "Generate JAR filename based on Flink version"
  [flink-version]
  (format "target/%s-%s-flink-%s.jar" (name lib) version flink-version))

(defn- get-basis
  "Create basis with the appropriate Flink alias"
  [flink-version]
  (let [alias-kw (keyword (str "flink-" flink-version))]
    (b/create-basis {:project "deps.edn"
                     :aliases [alias-kw]})))

(defn clean [_]
  (b/delete {:path "target"}))

(defn uber
  "Build uber JAR for a specific Flink version.
   Usage: clojure -T:build uber
          clojure -T:build uber :flink-version '\"1.18\"'
          clojure -T:build uber :flink-version '\"1.19\"'
          clojure -T:build uber :flink-version '\"1.20\"'
          clojure -T:build uber :flink-version '\"2.1\"'"
  [opts]
  (let [flink-version (get-flink-version opts)
        uber-file (uber-file-name flink-version)
        basis (get-basis flink-version)]
    (println (str "Building Metabase Flink SQL Driver for Flink " flink-version))
    (println (str "  JDBC Driver: " (get flink-versions flink-version)))
    (println (str "  Output: " uber-file))
    (clean nil)
    ;; Copy source files and resources (not compiled - Metabase compiles at runtime)
    (b/copy-dir {:src-dirs ["src" "resources"]
                 :target-dir class-dir})
    ;; Create uber JAR with dependencies (Flink JDBC driver)
    ;; Skip Clojure compilation - Metabase provides the runtime environment
    (b/uber {:class-dir class-dir
             :uber-file uber-file
             :basis basis
             :main nil
             ;; Exclude Clojure itself as Metabase provides it
             :exclude ["^clojure/.*"]})
    (println "Build complete!")))

(defn uber-all
  "Build uber JARs for all supported Flink versions.
   Usage: clojure -T:build uber-all"
  [_]
  (doseq [flink-version (keys flink-versions)]
    (println (str "\n=== Building for Flink " flink-version " ==="))
    (uber {:flink-version flink-version}))
  (println "\n=== All builds complete! ===")
  (println "Built JARs:")
  (doseq [flink-version (sort (keys flink-versions))]
    (println (str "  - " (uber-file-name flink-version)))))
