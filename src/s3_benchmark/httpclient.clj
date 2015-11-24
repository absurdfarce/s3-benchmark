(ns s3-benchmark.httpclient
  (:require [clj-time.core :as time]
            [clj-time.format :as format]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [s3-benchmark.util :as util])
  (:import  [com.google.common.net MediaType]
            [org.apache.http HttpHeaders HttpHost]
            [org.apache.http.client.methods HttpGet HttpPut]
            [org.apache.http.entity InputStreamEntity]
            [org.apache.http.protocol HttpProcessorBuilder]
            [org.apache.http.impl.client HttpClients]
            [org.apache.http.impl.conn DefaultProxyRoutePlanner]
            [org.apache.http.util VersionInfo]
            [javax.crypto Mac]
            [javax.crypto.spec SecretKeySpec]
            [org.apache.commons.codec.binary Base64]))

;; Use a custom HttpProcessor in order to avoid adding any extraneous headers.  httpclient can be a bit
;; aggresive on that front, and AWS authentication is based on headers included in the request... so
;; we need to exercise strict control over which headers are and aren't used.
(def client (-> (HttpClients/custom)
                ;; In case you want to use a proxy
                ;;(.setRoutePlanner (DefaultProxyRoutePlanner. (HttpHost. "localhost" 8080)))
                (.setHttpProcessor (-> (HttpProcessorBuilder/create) (.build)))
                (.build)))

(def user-agent (VersionInfo/getUserAgent "Apache-HttpClient" "org.apache.http.client" (class HttpClients)))

(defn- build-authorization-string
  [verb bucket k date-str]
  (let [algo "HmacSHA1"
        key (SecretKeySpec. (.getBytes (System/getenv "AWS_SECRET_KEY")) algo)
        mac (doto (Mac/getInstance algo)
              (.init key))
        canonical-resource (format "/%s/%s" bucket k)
        str-to-sign (format "%s\n\n\n%s\n%s" verb (string/trim date-str) (string/trim canonical-resource))
        signature (.doFinal mac (.getBytes str-to-sign))]
    (format "AWS %s:%s" (System/getenv "AWS_ACCESS_KEY") (Base64/encodeBase64String signature))))

(defn upload-file
  [credentials bucket file]
  (let [file-obj (io/as-file file)]
    (with-open [file-content (io/input-stream file-obj)]
      (let [req (doto (HttpPut. (format "http://%s.s3.amazonaws.com/%s" bucket (.getName file-obj)))
                  (.setHeader HttpHeaders/HOST (format "%s.s3.amazonaws.com" bucket))
                  (.setHeader HttpHeaders/DATE (format/unparse (format/formatters :rfc822) (time/now)))
                  (.setHeader HttpHeaders/CONTENT_TYPE (str MediaType/OCTET_STREAM))
                  (.setEntity (InputStreamEntity. file-content)))]
        (log/info "Request: " (str req))
        (log/info "Headers: " (map #(str (.getName %1) " -> " (.getValue %1)) (.getAllHeaders req)))
        (with-open [resp (.execute client req)]
          (log/info "Response from Amazon (upload): " (.getStatusLine resp)))))))

(defn download-file
  [credentials bucket k dest-dir]
  (let [date-str (format/unparse (format/formatters :rfc822) (time/now))
        req (doto (HttpGet. (format "http://%s.s3.amazonaws.com/%s" bucket k))
              (.setHeader HttpHeaders/HOST (format "%s.s3.amazonaws.com" bucket))
              (.setHeader HttpHeaders/DATE date-str)
              (.setHeader HttpHeaders/AUTHORIZATION (build-authorization-string "GET" bucket k date-str))
              (.setHeader HttpHeaders/USER_AGENT user-agent))
        tmp-file (io/file dest-dir k)]
    (with-open [resp (.execute client req)]
      (let [entity-stream (-> resp
                              (.getEntity)
                              (.getContent))]
        (io/copy entity-stream tmp-file)))
    (.length tmp-file)))
