(defproject s3_benchmark "0.1.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/data.generators "0.1.2"]
                 [org.apache.jclouds/jclouds-blobstore "1.8.0" :exclusions [org.apache.jclouds/jclouds-core]]
                 [arre.jclouds/jclouds-core "1.8.0"]
		 [arre.jclouds.provider/aws-s3 "1.8.0" :exclusions [org.apache.jclouds/jclouds-core]]
                 [org.clojure/tools.logging "0.3.1"]
                 [amazonica "0.3.33"]
                 [clj-time "0.11.0"]
                 [commons-io "2.4"]]
  :main s3-benchmark.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
