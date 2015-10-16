(ns s3-benchmark.core
  (:require [clojure.tools.logging :as log]
            [clj-time.core :as time]
            [clj-time.format :as format]
            [s3-benchmark.conf :as conf]
            [s3-benchmark.amazonica :as amazonica]
            [s3-benchmark.jclouds :as jclouds]
            [s3-benchmark.util :as util]))

(defn- single-file-test [{:keys [creds, bucket, chunk-size, chunk-count, upload-fn, download-fn] :as test-params}]
  " {
      :timestamp 2015-10-03 09:12:22 CDT
      :result 'success'
      :exception nil
      :params {}
      :transfers [
        {:name 'upload' :wall-time 0 :free-mem-delta 0 :total-mem-delta 0}
        {...}]
    }"
  (let [local-tmp-file (util/generate-file chunk-size chunk-count)
        tmpdir (java.io.File. (System/getProperty "java.io.tmpdir") (util/random-uuid-string)) ;; unique place to download
        downloaded-file (java.io.File. tmpdir (.getName local-tmp-file))
        test-data (transient {:timestamp (format/unparse (format/formatters :rfc822) (time/now))
                              :params (dissoc test-params :creds :upload-fn :download-fn)
                              :test-file {:name (.getAbsolutePath local-tmp-file)
                                          :size (.length local-tmp-file)}})]
    (try
      (do
        (assoc! test-data
                :transfers
                (-> []
                    (util/record "upload" #(upload-fn creds bucket local-tmp-file))
                    (util/record "download" #(download-fn creds bucket local-tmp-file tmpdir))))
        (util/verify-files-match local-tmp-file downloaded-file)
        (.delete downloaded-file)
        (.delete local-tmp-file)
        (assoc! test-data :result "success" :exception nil))
      (catch Throwable ex (assoc! test-data :result "error" :exception (.getMessage ex))))
    (persistent! test-data)))


(defn- multiple-file-test
  [test-runs test-params]
  (for [i (range test-runs)]
    (do
      (log/infof "starting iteration %s" i)
      (Thread/sleep (util/random-long 10000 90000))
      (single-file-test test-params))))


(defn build-test-executor
  "Usage:   (run-build-test-executor test-fn & test-fn-args)
   Example: ((run-build-test-executor single-file-test {...}) 'single-test-run.edn')

  Returns a function that when invoked with a result-file will execute test-fn with test-fn-args
  and write any resulting data structure returned to the result-file in EDN format. This allows
  for the definition of a number of repeatable test cases that save the results to disk for further
  analysis."
  [test-fn & test-fn-args]
  (fn [result-file]
    (let [result (apply test-fn test-fn-args)]
      (spit (java.io.File. conf/default-report-dir result-file)
            (with-out-str (pr result))))))


;; Chunk Size / Chunk Count Table
;;
;;                                      chunk-size      chunk-count
;; small files    => 256K to 10MB       256 - 512       1000 - 19532
;; large files    => 50MB to 200MB      1024 - 2048     48829 - 97657
;; huge files     => 500MB to 1GB       4096 - 8192     122071 - 122071
(def test-cases {:amazonica {:small-files           (build-test-executor multiple-file-test 25 {:creds       conf/default-creds
                                                                                                :bucket      conf/default-bucket
                                                                                                :chunk-size  (util/random-long 256 512)
                                                                                                :chunk-count (util/random-long 1000 19532)
                                                                                                :upload-fn   amazonica/upload-file
                                                                                                :download-fn amazonica/download-file})
                             :large-files           (build-test-executor multiple-file-test 25 {:creds       conf/default-creds
                                                                                                :bucket      conf/default-bucket
                                                                                                :chunk-size  (util/random-long 1024 2048)
                                                                                                :chunk-count (util/random-long 48829 97657)
                                                                                                :upload-fn   amazonica/upload-file
                                                                                                :download-fn amazonica/download-file})
                             :huge-files            (build-test-executor multiple-file-test 25 {:creds       conf/default-creds
                                                                                                :bucket      conf/default-bucket
                                                                                                :chunk-size  (util/random-long 4096 8192)
                                                                                                :chunk-count 122071
                                                                                                :upload-fn   amazonica/upload-file
                                                                                                :download-fn amazonica/download-file})
                             :single-small-file     (build-test-executor single-file-test {:creds       conf/default-creds
                                                                                           :bucket      conf/default-bucket
                                                                                           :chunk-size  (util/random-long 256 512)
                                                                                           :chunk-count (util/random-long 1000 19532)
                                                                                           :upload-fn   amazonica/upload-file
                                                                                           :download-fn amazonica/download-file})
                             :single-large-file     (build-test-executor single-file-test {:creds       conf/default-creds
                                                                                           :bucket      conf/default-bucket
                                                                                           :chunk-size  (util/random-long 1024 2048)
                                                                                           :chunk-count (util/random-long 48829 97657)
                                                                                           :upload-fn   amazonica/upload-file
                                                                                           :download-fn amazonica/download-file})
                             :single-huge-file      (build-test-executor single-file-test {:creds       conf/default-creds
                                                                                           :bucket      conf/default-bucket
                                                                                           :chunk-size  (util/random-long 4096 8192)
                                                                                           :chunk-count 122071
                                                                                           :upload-fn   amazonica/upload-file
                                                                                           :download-fn amazonica/download-file})
                             :nio-small-files       (build-test-executor multiple-file-test 25 {:creds       conf/default-creds
                                                                                                :bucket      conf/default-bucket
                                                                                                :chunk-size  (util/random-long 256 512)
                                                                                                :chunk-count (util/random-long 1000 19532)
                                                                                                :upload-fn   amazonica/upload-file-nio
                                                                                                :download-fn amazonica/download-file-nio})
                             :nio-large-files       (build-test-executor multiple-file-test 25 {:creds       conf/default-creds
                                                                                                :bucket      conf/default-bucket
                                                                                                :chunk-size  (util/random-long 1024 2048)
                                                                                                :chunk-count (util/random-long 48829 97657)
                                                                                                :upload-fn   amazonica/upload-file-nio
                                                                                                :download-fn amazonica/download-file-nio})
                             :nio-huge-files        (build-test-executor multiple-file-test 25 {:creds       conf/default-creds
                                                                                                :bucket      conf/default-bucket
                                                                                                :chunk-size  (util/random-long 4096 8192)
                                                                                                :chunk-count 122071
                                                                                                :upload-fn   amazonica/upload-file-nio
                                                                                                :download-fn amazonica/download-file-nio})
                             :nio-single-small-file (build-test-executor single-file-test {:creds       conf/default-creds
                                                                                           :bucket      conf/default-bucket
                                                                                           :chunk-size  (util/random-long 256 512)
                                                                                           :chunk-count (util/random-long 1000 19532)
                                                                                           :upload-fn   amazonica/upload-file-nio
                                                                                           :download-fn amazonica/download-file-nio})
                             :nio-single-large-file (build-test-executor single-file-test {:creds       conf/default-creds
                                                                                           :bucket      conf/default-bucket
                                                                                           :chunk-size  (util/random-long 1024 2048)
                                                                                           :chunk-count (util/random-long 48829 97657)
                                                                                           :upload-fn   amazonica/upload-file-nio
                                                                                           :download-fn amazonica/download-file-nio})
                             :nio-single-huge-file  (build-test-executor single-file-test {:creds       conf/default-creds
                                                                                           :bucket      conf/default-bucket
                                                                                           :chunk-size  (util/random-long 4096 8192)
                                                                                           :chunk-count 122071
                                                                                           :upload-fn   amazonica/upload-file-nio
                                                                                           :download-fn amazonica/download-file-nio})}
                 :jclouds   {:small-files       (build-test-executor multiple-file-test 25 {:creds       conf/default-creds
                                                                                            :bucket      conf/default-bucket
                                                                                            :chunk-size  (util/random-long 256 512)
                                                                                            :chunk-count (util/random-long 1000 19532)
                                                                                            :upload-fn   jclouds/upload-file
                                                                                            :download-fn jclouds/download-file})
                             :large-files       (build-test-executor multiple-file-test 25 {:creds       conf/default-creds
                                                                                            :bucket      conf/default-bucket
                                                                                            :chunk-size  (util/random-long 1024 2048)
                                                                                            :chunk-count (util/random-long 48829 97657)
                                                                                            :upload-fn   jclouds/upload-file
                                                                                            :download-fn jclouds/download-file})
                             :huge-files        (build-test-executor multiple-file-test 25 {:creds       conf/default-creds
                                                                                            :bucket      conf/default-bucket
                                                                                            :chunk-size  (util/random-long 4096 8192)
                                                                                            :chunk-count 122071
                                                                                            :upload-fn   jclouds/upload-file
                                                                                            :download-fn jclouds/download-file})
                             :single-small-file (build-test-executor single-file-test {:creds       conf/default-creds
                                                                                       :bucket      conf/default-bucket
                                                                                       :chunk-size  (util/random-long 256 512)
                                                                                       :chunk-count (util/random-long 1000 19532)
                                                                                       :upload-fn   jclouds/upload-file
                                                                                       :download-fn jclouds/download-file})
                             :single-large-file (build-test-executor single-file-test {:creds       conf/default-creds
                                                                                       :bucket      conf/default-bucket
                                                                                       :chunk-size  (util/random-long 1024 2048)
                                                                                       :chunk-count (util/random-long 48829 97657)
                                                                                       :upload-fn   jclouds/upload-file
                                                                                       :download-fn jclouds/download-file})
                             :single-huge-file  (build-test-executor single-file-test {:creds       conf/default-creds
                                                                                       :bucket      conf/default-bucket
                                                                                       :chunk-size  (util/random-long 4096 8192)
                                                                                       :chunk-count 122071
                                                                                       :upload-fn   jclouds/upload-file
                                                                                       :download-fn jclouds/download-file})}})

(defn run
  "REPL function that runs a particular test that was defined with a library type and test case name from the
  global map. The resulting data structure is then written out with a generated filename to the configured
  report directory."
  [lib-type test-case]
  (let [filename-format (format/formatter "YYYYMMdd_HHmm")
        date-time (format/unparse filename-format (time/now))
        output-file (str date-time "_" (util/keyword->filename lib-type) "_" (util/keyword->filename test-case) ".edn")
        test-fn (-> test-cases
                    (get lib-type)
                    (get test-case))]
    (test-fn output-file)))
