(ns s3-benchmark.core
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [clj-time.core :as time]
            [clj-time.format :as format]
            [s3-benchmark.analyze :as analyze]
            [s3-benchmark.conf :as conf]
            [s3-benchmark.amazonica :as amazonica]
            [s3-benchmark.httpclient :as httpclient]
            [s3-benchmark.jclouds :as jclouds]
            [s3-benchmark.util :as util])
  (:import [com.google.common.io Files]))

; ---------------------------------------- Stage utilities ----------------------------------------
(defn- timing-stage-decorator
  [name stage-fn]
  (fn [state]
    (let [start-time (System/currentTimeMillis)
          runtime (Runtime/getRuntime)
          start-free-mem (.freeMemory runtime)
          start-total-mem (.totalMemory runtime)]
      (try
        (assoc (apply stage-fn [state])
               (keyword (str name "-wall-time")) (- (System/currentTimeMillis) start-time)
               (keyword (str name "-free-mem-delta")) (- (.freeMemory runtime) start-free-mem)
               (keyword (str name "-total-mem-delta")) (- (.totalMemory runtime) start-total-mem))
        (catch Exception e
          (log/info "Exception in test function " e)
          state)))))

; ---------------------------------------- Stage definitions ---------------------------------------- 
(defn- generate-upload-stage
  [params state]
  (log/info "Generating upload file, input state: " state)
  (let [tmp-dir (Files/createTempDir)
        tmp-file (util/generate-file tmp-dir (:chunk-size params) (:chunk-count params))
        tmp-crc (util/compute-crc32 (io/input-stream tmp-file))]
    (assoc state :upload-file tmp-file :upload-crc tmp-crc)))

(defn- upload-adapter-stage
  "Adapter for stage API into upload fn API" 
  [upload-fn params state]
  (log/info "Uploading file, input state: " state)
  (upload-fn (:creds params) (:bucket params) (:upload-file state))
  ;; Destroy references to FS state going forward, but since every upload fn uses getName() output as key
  ;; record that for future ops here.
  (assoc (dissoc state :upload-file) :key (.getName (:upload-file state))))

(defn- download-adapter-stage
  "Adapter for stage API into download fn API"
  [download-fn params state]
  (log/info "Downloading file, input state: " state)
  (let [tmp-dir (Files/createTempDir)
        tmp-file (io/file tmp-dir (:key state))]
    (download-fn (:creds params) (:bucket params) (:key state) tmp-dir)
    (assoc state :download-crc (util/compute-crc32 (io/input-stream tmp-file)))))

; ---------------------------------------- Param handling ----------------------------------------
(defn- build-lib-params
  [lib-type]
  (cond
    (= lib-type :amazonica) {:upload-fn amazonica/upload-file :download-fn amazonica/download-file}
    (= lib-type :amazonica-nio) {:upload-fn amazonica/upload-file-nio :download-fn amazonica/download-file-nio}
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
        generate-stage (partial generate-upload-stage params)
        upload-fn (partial upload-adapter-stage (:upload-fn params) params)
        upload-stage (timing-stage-decorator "upload" upload-fn)
        download-fn (partial download-adapter-stage (:download-fn params) params)
        download-stage (timing-stage-decorator "download" download-fn)]
    (apply (comp download-stage upload-stage generate-stage) [{}])))
