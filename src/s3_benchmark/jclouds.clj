(ns s3-benchmark.jclouds
  (:require [clojure.java.io :as io]
            [org.jclouds.blobstore2 :as blobstore2]
            [s3-benchmark.util :as util])
  (:import [com.google.common.net MediaType]))

(defn upload-file
  [credentials bucket file]
  (let [file-obj (io/as-file file)
        aws-blobstore (blobstore2/blobstore "aws-s3" (:access-key credentials) (:secret-key credentials))]
    (with-open [file-content (io/input-stream file-obj)]
      (blobstore2/put-blob aws-blobstore
                           bucket
                           (blobstore2/blob (.getName file-obj)
                                            :payload file-content
                                            :content-type (str MediaType/OCTET_STREAM)
                                            :content-length (.length file-obj))))))

(defn download-file
  [credentials bucket k dest-dir]
  (let [dest-dir-obj (io/as-file dest-dir)
        aws-blobstore (blobstore2/blobstore "aws-s3" (:access-key credentials) (:secret-key credentials))
        tmp-file (io/file dest-dir k)]
    (util/ensure-dir-exists dest-dir-obj)
    (with-open [s3-object-stream (blobstore2/get-blob-stream aws-blobstore bucket k)]
      (io/copy s3-object-stream tmp-file))
    (.length tmp-file)))
