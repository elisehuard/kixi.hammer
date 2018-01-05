(set-env!
 :resource-paths #{"src"}
 :dependencies '[[org.clojure/clojure "1.9.0"]
                 [pjstadig/reducible-stream "0.1.3"]
                 [org.clojure/data.csv "0.1.4"]
                 [clojure.java-time "0.3.1"]])

(task-options!
 pom {:project 'kixi.hammer
      :version "0.1.0-SNAPSHOT"})
