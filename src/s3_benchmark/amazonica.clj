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


(defn upload-file-nio
  [credentials bucket file]
  (let [path-obj (.toPath (io/as-file file))
        open-option-array (make-array java.nio.file.OpenOption 1)
        _ (aset open-option-array 0 java.nio.file.StandardOpenOption/READ)]
    (with-open [file-content (java.nio.file.Files/newInputStream path-obj open-option-array)]
      (put-object credentials :bucket-name bucket
                              :key (.toString (.getFileName path-obj))
                              :metadata {:content-length (java.nio.file.Files/size path-obj)}
                              :input-stream file-content))))


(defn download-file
  [credentials bucket key dest-dir]
  (let [dest-dir-obj (io/as-file dest-dir)
        s3-object (get-object credentials :bucket-name bucket
                                          :key key)]
    (util/ensure-dir-exists dest-dir-obj)
    (with-open [s3-object-stream (:object-content s3-object)]
      (io/copy s3-object-stream
               (java.io.File. dest-dir key)))))


(defn download-file-nio
  [credentials bucket key dest-dir]
  (let [dest-dir-obj (io/as-file dest-dir)
        dest-file-path (.toPath (java.io.File. dest-dir-obj key))
        copy-option-array (make-array java.nio.file.CopyOption 1)
        _ (aset copy-option-array 0 java.nio.file.StandardCopyOption/REPLACE_EXISTING)
        s3-object (get-object credentials :bucket-name bucket
                                          :key key)]
    (util/ensure-dir-exists dest-dir-obj)
    (with-open [s3-object-stream (:object-content s3-object)]
      (java.nio.file.Files/copy s3-object-stream dest-file-path copy-option-array))))


