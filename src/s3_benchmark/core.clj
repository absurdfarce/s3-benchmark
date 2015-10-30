(ns s3-benchmark.core
  (:require [clojure.tools.logging :as log]
            [clj-time.core :as time]
            [clj-time.format :as format]
            [s3-benchmark.analyze :as analyze]
            [s3-benchmark.conf :as conf]
            [s3-benchmark.amazonica :as amazonica]
            [s3-benchmark.httpclient :as httpclient]
            [s3-benchmark.jclouds :as jclouds]
            [s3-benchmark.util :as util]))

(defn- single-file-test [{:keys [creds bucket chunk-size chunk-count upload-fn download-fn] :as test-params}]
  " {
      :timestamp 2015-10-03 09:12:22 CDT
      :result 'success'
      :exception nil
      :params {}
      :transfers [
        {:name 'upload' :wall-time 0 :free-mem-delta 0 :total-mem-delta 0}
        {...}]
    }"
  (println "Running test, chunk size: " chunk-size ", chunk count: " chunk-count)
  (let [local-tmp-file (util/generate-file chunk-size chunk-count)
        tmpdir (java.io.File. (System/getProperty "java.io.tmpdir") (util/random-uuid-string))
        downloaded-file (java.io.File. tmpdir (.getName local-tmp-file))
        test-data {:timestamp (format/unparse (format/formatters :rfc822) (time/now))
                              :params (dissoc test-params :creds :upload-fn :download-fn)
                              :test-file {:name (.getName local-tmp-file)
                                          :size (.length local-tmp-file)}}
        transfers [(when upload-fn
                     (util/record "upload" upload-fn [creds bucket local-tmp-file]))
                   (when download-fn
                     (util/record "download" download-fn [creds bucket (.getName local-tmp-file) tmpdir]))]]
    (util/verify-files-match local-tmp-file downloaded-file)
    (.delete downloaded-file)
    (.delete local-tmp-file)
    (assoc test-data :transfers transfers)))

(defn- multiple-file-test
  [test-runs test-params]
  (for [i (range test-runs)]
    (do
      (log/infof "starting iteration %s" i)
      (Thread/sleep (util/random-long 10000 90000))
      (single-file-test test-params))))

(defn run-test
  "REPL function that runs a particular test that was defined with a library type and test case name from the
  global map. The resulting data structure is then written out with a generated filename to the configured
  report directory."
  [lib-type test-size & [num-files]]
  (let [lib-params (cond (= lib-type :amazonica) {:upload-fn amazonica/upload-file :download-fn amazonica/download-file}
                         (= lib-type :amazonica-nio) {:upload-fn amazonica/upload-file-nio :download-fn amazonica/download-file-nio}
                         (= lib-type :jclouds) {:upload-fn jclouds/upload-file :download-fn jclouds/download-file}
                         (= lib-type :httpclient) {:upload-fn httpclient/upload-file :download-fn httpclient/download-file}) 
        ;; Chunk Size / Chunk Count Table
        ;;                                      chunk-size      chunk-count
        ;; small files    => 256K to 10MB       256 - 512       1000 - 19532
        ;; large files    => 50MB to 200MB      1024 - 2048     48829 - 97657
        ;; huge files     => 500MB to 1GB       4096 - 8192     122071 - 122071
        size-params (cond (= test-size :small) {:chunk-size  (util/random-long 256 512) :chunk-count (util/random-long 1000 19532)}
                        (= test-size :large) {:chunk-size  (util/random-long 1024 2048) :chunk-count (util/random-long 48829 97657)}
                        (= test-size :huge) {:chunk-size  (util/random-long 4096 8192) :chunk-count 122071}
                        (= test-size :really-small) {:chunk-size 256 :chunk-count 40})
        params (merge {:creds conf/default-creds :bucket conf/default-bucket} lib-params size-params)
        file-count (or num-files 1)
        results (if (> file-count 1)
                  (apply multiple-file-test [file-count params])
                  (apply single-file-test [params]))
        download-data (analyze/transform-download-data results)
        download-speed (analyze/compute-download-speed download-data)]
    (println "Raw results: " results)
    (println "Download data: " download-data)
    (println "Download speed: " download-speed)
    download-speed))
