(ns s3-benchmark.core
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [clj-time.core :as time]
            [clj-time.format :as format]
            [s3-benchmark.conf :as conf]
            [s3-benchmark.amazonica :as amazonica]
            [s3-benchmark.httpclient :as httpclient]
            [s3-benchmark.jclouds :as jclouds]
            [s3-benchmark.util :as util])
  (:import [com.google.common.base Stopwatch]
           [com.google.common.io Files]
           [java.util.concurrent TimeUnit]
           [org.apache.commons.io FileUtils])
  (:gen-class))

; ---------------------------------------- Stage definitions ---------------------------------------- 
(defn- generate-upload-stage
  [params state]
  (let [tmp-dir (Files/createTempDir)
        tmp-file (util/generate-file tmp-dir (:chunk-size params) (:chunk-count params))]
    (assoc state
           :upload-file tmp-file
           :upload-crc (util/compute-crc32 (io/input-stream tmp-file))
           :upload-size (.length tmp-file)
           :upload-delete-fn #(FileUtils/deleteQuietly tmp-dir))))

(defn- upload-adapter-stage
  "Adapter for stage API into upload fn API" 
  [upload-fn params state]
  ;; Destroy references to FS state going forward, but since every upload fn uses getName() output as key
  ;; record that for future ops here.
  (let [base-state (dissoc state :upload-file :upload-delete-fn)]
    (try
      (upload-fn (:creds params) (:bucket params) (:upload-file state))
      (assoc base-state :key (.getName (:upload-file state)))
      (catch Exception e
        (assoc base-state :upload-exception e))
      (finally
        (apply (:upload-delete-fn state) nil)))))

(defn- download-adapter-stage
  "Adapter for stage API into download fn API"
  [download-fn params state]
  (let [tmp-dir (Files/createTempDir)
        tmp-file (io/file tmp-dir (:key state))]
    (try
      (let [download-size (download-fn (:creds params) (:bucket params) (:key state) tmp-dir)]
        (assoc state
               :download-crc (util/compute-crc32 (io/input-stream tmp-file))
               :download-size download-size))
      (catch Exception e
        (assoc state :download-exception e))
      (finally
        (FileUtils/deleteQuietly tmp-dir)))))

(defn- postprocess-stage
  "Do a bit of calculation based on the current state"
  [params state]
  (letfn [(compute-mb-sec [file-size-bytes wall-time-msec]
            (let [file-size-mb (/ file-size-bytes 1000000M)
                  wall-time-sec (/ wall-time-msec 1000M)]
              (with-precision 5 (/ file-size-mb wall-time-sec))))]
    (cond-> state
      (contains? state :upload-wall-time) (assoc :upload-mb-sec (compute-mb-sec
                                                                 (* (:chunk-size params) (:chunk-count params))
                                                                 (:upload-wall-time state)))
      (and (contains? state :download-wall-time)
           (contains? state :download-size)) (assoc :download-mb-sec (compute-mb-sec
                                                                      (:download-size state)
                                                                      (:download-wall-time state)))
      (and (contains? state :upload-crc)
           (contains? state :download-crc)) (assoc :crc-match (= (:upload-crc state) (:download-crc state))))))

; ---------------------------------------- Stage utilities ----------------------------------------
(defn- apply-with-stopwatch
  [the-fn args]
  (let [stopwatch (Stopwatch/createUnstarted)]
    (.start stopwatch)
    (let [rv (apply the-fn args)]
      (.stop stopwatch)
      [rv stopwatch])))

(defn- timing-stage-decorator
  [name stage-fn]
  (fn [state]
    (let [runtime (Runtime/getRuntime)
          start-free-mem (.freeMemory runtime)
          start-total-mem (.totalMemory runtime)]
      (try
        (let [[stage-state stopwatch] (apply-with-stopwatch stage-fn [state])]
          (assoc stage-state
                 (keyword (str name "-wall-time")) (.elapsed stopwatch TimeUnit/MILLISECONDS)
                 (keyword (str name "-free-mem-delta")) (- (.freeMemory runtime) start-free-mem)
                 (keyword (str name "-total-mem-delta")) (- (.totalMemory runtime) start-total-mem)))
        (catch Exception e
          (log/info "Exception in test function" e)
          state)))))

(defn- composed-upload-stage
  "Return a function which represents the composed upload ops (file generation + upload) for a given set of params"
  [params]
  (let [generate-stage (partial generate-upload-stage params)
        upload-fn (partial upload-adapter-stage (:upload-fn params) params)
        upload-stage (timing-stage-decorator "upload" upload-fn)]
    (comp upload-stage generate-stage)))

(defn- composed-download-stage
  "Return a function which represents the composed upload ops (file generation + upload) for a given set of params"
  [params]
  (let [download-fn (partial download-adapter-stage (:download-fn params) params)
        download-stage (timing-stage-decorator "download" download-fn)
        postprocess (partial postprocess-stage params)]
    (comp postprocess download-stage)))

; ---------------------------------------- Param handling ----------------------------------------
(defn- build-lib-params
  [lib-type]
  (cond
    (= lib-type :amazonica) {:upload-fn amazonica/upload-file :download-fn amazonica/download-file}
    (= lib-type :jclouds) {:upload-fn jclouds/upload-file :download-fn jclouds/download-file}
    (= lib-type :httpclient) {:upload-fn httpclient/upload-file :download-fn httpclient/download-file}))

;; Chunk Size / Chunk Count Table
;;                                      chunk-size      chunk-count
;; small files    => 256K to 10MB       256 - 512       1000 - 19532
;; large files    => 50MB to 200MB      1024 - 2048     48829 - 97657
;; huge files     => 500MB to 1GB       4096 - 8192     122071 - 122071
(defn- build-size-params
  [test-size]
  (cond
    (= test-size :small) {:chunk-size  (util/random-long 256 512) :chunk-count (util/random-long 1000 19532)}
    (= test-size :large) {:chunk-size  (util/random-long 1024 2048) :chunk-count (util/random-long 48829 97657)}
    (= test-size :huge) {:chunk-size  (util/random-long 4096 8192) :chunk-count 122071}
    (= test-size :really-small) {:chunk-size 256 :chunk-count 40}))

; ---------------------------------------- REPL fns ---------------------------------------- 
(defn upload-download-test
  "REPL function that runs a complete upload + download test"
  [lib-type test-size]
  (let [lib-params (build-lib-params lib-type) 
        size-params (build-size-params test-size)
        params (merge {:creds conf/default-creds :bucket conf/default-bucket} lib-params size-params)
        upload-stage (composed-upload-stage params)
        download-stage (composed-download-stage params)]
    (apply (comp download-stage upload-stage) [{}])))

(defn download-test
  "REPL function that runs a complete download test"
  [lib-type k]
  (let [lib-params (build-lib-params lib-type) 
        params (merge {:creds conf/default-creds :bucket conf/default-bucket} lib-params)]
    (apply (composed-download-stage params) [{:key k}])))

(defn download-only-test
  "REPL function that runs only the download test (no postprocessing)"
  [lib-type k]
  (let [lib-params (build-lib-params lib-type) 
        params (merge {:creds conf/default-creds :bucket conf/default-bucket} lib-params)]
    (apply (partial download-adapter-stage (:download-fn params) params) [{:key k}])))

(defn -main [& args]
  (let [[lib-type key] args]
    (println (format "Download test: lib %s, key %s" lib-type key))
    (println (download-test (keyword lib-type) key))))
