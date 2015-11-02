(ns s3-benchmark.httpclient
  (:require [clj-time.core :as time]
            [clj-time.format :as format]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [s3-benchmark.util :as util])
  (:import  [com.google.common.net MediaType]
            [org.apache.http HttpHeaders]
            [org.apache.http.client.methods HttpGet HttpPut]
            [org.apache.http.entity InputStreamEntity]
            [org.apache.http.impl.client HttpClients]))

(def client (HttpClients/createDefault))

(defn upload-file
  [credentials bucket file]
  (let [file-obj (io/as-file file)]
    (with-open [file-content (io/input-stream file-obj)]
      (let [req (doto (HttpPut. (format "http://%s.s3.amazonaws.com/%s" bucket (.getName file-obj)))
                  (.setHeader HttpHeaders/HOST (format "http://%s.s3.amazonaws.com" bucket))
                  (.setHeader HttpHeaders/DATE (format/unparse (format/formatters :rfc822) (time/now)))
                  (.setHeader HttpHeaders/CONTENT_TYPE (str MediaType/OCTET_STREAM))
                  (.setEntity (InputStreamEntity. file-content)))]
        (log/info "Request: " (str req))
        (log/info "Headers: " (map #(str (.getName %1) "-" (.getValue %1)) (.getAllHeaders req)))
        (with-open [resp (.execute client req)]
          (log/info "Response from Amazon (upload): " (.getStatusLine resp)))))))

(defn download-file
  [credentials bucket k dest-dir]
  (let [req (HttpGet. (format "http://%s.s3.amazonaws.com/%s" bucket k))]
    (with-open [resp (.execute client req)]
      (let [entity-stream (-> resp
                              (.getEntity)
                              (.getContent))]
        (io/copy entity-stream (io/file dest-dir k))))))
