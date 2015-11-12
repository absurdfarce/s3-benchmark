(defproject s3_benchmark "0.1.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/data.generators "0.1.2"]
                 [org.apache.jclouds/jclouds-blobstore "1.8.0" :exclusions [org.apache.jclouds/jclouds-core]]
                 [arre.jclouds/jclouds-core "1.8.0"]
		 [arre.jclouds.provider/aws-s3 "1.8.0" :exclusions [org.apache.jclouds/jclouds-core]]
                 [org.clojure/tools.logging "0.3.1"]
                 [clj-time "0.11.0"]
                 [commons-io "2.4"]
                 ;; Fix Apache HC version at 4.3.6 to avoid any conflicts between amazonica and jclouds drivers
                 [org.apache.httpcomponents/httpclient "4.3.6"]
                 [amazonica "0.3.33" :exclusions [org.apache.httpcomponents/httpclient]]
                 [org.apache.jclouds.driver/jclouds-apachehc "1.8.0" :exclusions [org.apache.httpcomponents/httpclient org.apache.jclouds/jclouds-core]]]
  :main s3-benchmark.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
