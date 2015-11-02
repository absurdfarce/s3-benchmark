(ns s3-benchmark.amazonica
  (:use [amazonica.aws.s3])
  (:require [clojure.java.io :as io]
            [s3-benchmark.util :as util]))

(defn upload-file
  [credentials bucket file]
  (let [file-obj (io/as-file file)]
    (with-open [file-content (io/input-stream file-obj)]
      (put-object credentials :bucket-name bucket
                              :key (.getName file-obj)
                              :metadata {:content-length (.length file-obj)}
                              :input-stream file-content))))

(defn download-file
  [credentials bucket k dest-dir]
  (let [s3-object (get-object credentials :bucket-name bucket :key k)
        tmp-file (io/file dest-dir k)]
    (util/ensure-dir-exists dest-dir)
    (with-open [s3-object-stream (:object-content s3-object)]
      (io/copy s3-object-stream tmp-file))
    (.length tmp-file)))
