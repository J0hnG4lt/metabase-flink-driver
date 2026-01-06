(ns build
  (:require [clojure.tools.build.api :as b]))

(def lib 'com.github.j0hng4lt/metabase-flink-driver)
(def version "1.0.0-SNAPSHOT")
(def class-dir "target/classes")
(def uber-file (format "target/%s-%s.jar" (name lib) version))

;; Include the Flink JDBC driver in the JAR
(def basis (delay (b/create-basis {:project "deps.edn"})))

(defn clean [_]
  (b/delete {:path "target"}))

(defn uber [_]
  (clean nil)
  ;; Copy source files and resources (not compiled - Metabase compiles at runtime)
  (b/copy-dir {:src-dirs ["src" "resources"]
               :target-dir class-dir})
  ;; Create uber JAR with dependencies (Flink JDBC driver)
  ;; Skip Clojure compilation - Metabase provides the runtime environment
  (b/uber {:class-dir class-dir
           :uber-file uber-file
           :basis @basis
           :main nil
           ;; Exclude Clojure itself as Metabase provides it
           :exclude ["^clojure/.*"]}))
