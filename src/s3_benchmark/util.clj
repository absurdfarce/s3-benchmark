(ns s3-benchmark.util
  "Collection of helper functions used to make writing the test cases and benchmarking code."
  (:require [clojure.java.io :as io]
            [clojure.data.generators :as generators]
            [clojure.string :as string]
            [clojure.tools.logging :as log])
  (:import [java.util.zip CRC32]))


(defn random-uuid-string
  "Usage: (random-uuod-string)

  This function is just a prettier way of using java.util.UUID to generate a new random
  UUID as a string value."
  []
  (.toString (java.util.UUID/randomUUID)))


(defn random-long
  "Usage:   (random-long lower-bound upper-bound)
   Example: (random-long 10 20)

  Generates a random number between lower-bound and upper-bound values as a long."
  [lower-bound upper-bound]
  (let [range (- upper-bound lower-bound)
        rnd-value (rand)]
    (+ lower-bound (long (* range rnd-value)))))


(defn write-random-bytes!
  "Generates a set of random bytes specified by num-bytes and writes them directly to the
  output-stream. This function does not close the stream when finished - that is the responsibility
  of the caller."
  [output-stream num-bytes]
  (.write output-stream (generators/byte-array generators/byte num-bytes)))


(defn generate-file
  "Generates a file of random bytes. The size of the file is controlled by specifying the chunk-size
  to write in a single pass and the num-chunks to write (this means total size is chunk-size * num-chunks).
  The file will be truncated before writing the random data."
  [chunk-size num-chunks]
  (let [file (java.io.File/createTempFile (random-uuid-string) ".dat")]
    (with-open [file-output-stream (io/output-stream file)]
      (dotimes [_ num-chunks]
        (write-random-bytes! file-output-stream chunk-size)))
    file))


(defn compute-crc32
  "Usage:   (compute-crc32 stream)
   Example: (compute-crc32 (io/input-stream '/etc/dse/cassandra.yaml'))

  Given an java.io.InputStream, computes the CRC32 checksum value as an integer
  for the data in the stream. This function does not close the stream so the caller
  should perform that operation."
  [stream]
  (let [crc32 (CRC32.)
        buffer (byte-array 2048)]
    (loop [bytes-read (.read stream buffer)]
      (if (< -1 bytes-read)
        (do
          (.update crc32 buffer 0 bytes-read)
          (recur (.read stream buffer)))
        (.getValue crc32)))))


(defn verify-files-match
  "Usage:   (verify-files-match f1 f2)
   Example: (verify-files-match '/etc/dse/cassandra.yaml' '/usr/local/cassandra/conf/cassandra.yaml')

  Verifies the contents of f1 and f2 are the same by computing the CRC32 check on each and then
  asserts that the checksums are the same. Raises an exception if the files are different."
  [f1 f2]
  (with-open [f1-stream (io/input-stream f1)
              f2-stream (io/input-stream f2)]
    (let [f1-crc (compute-crc32 f1-stream)
          f2-crc (compute-crc32 f2-stream)]
      (assert (= f1-crc f2-crc)
              (format "CHECKSUMS DO NOT MATCH: %s (CRC32 = %s), %s (CRC32 = %s)" f1 f1-crc f2 f2-crc)))))


(defn record
  "The record function executes the test-fn after capturing the wall time and some basic JVM memory
  stats and then computes the delta of these stats after the function executes. These stats are
  wrapped up in a map and then the map is added to the transfers vector."
  [name test-fn params]
  (let [start-time (System/currentTimeMillis)
        start-free-mem (.freeMemory (Runtime/getRuntime))
        start-total-mem (.totalMemory (Runtime/getRuntime))]
    (try
      (apply test-fn params)
      {:name name
       :wall-time (- (System/currentTimeMillis) start-time)
       :free-mem-delta (- (.freeMemory (Runtime/getRuntime)) start-free-mem)
       :total-mem-delta (- (.totalMemory (Runtime/getRuntime)) start-total-mem)}
      (catch Exception e
        (log/info "Exception in test function " e)
        nil))))

(defn keyword->filename
  "Converts a keyword into a name suitable for embedding in a filename. In addition to using the name
  function to obtain a string, dashes are replaced with underscore characters. Returns the modified
  name of the keyword."
  [keyword]
  (string/replace (name keyword) "-" "_"))


(defn ensure-dir-exists
  "Creates the given directory if it doesn't already exist."
  [^java.io.File dir-obj]
  (if-not (.exists dir-obj)
    (.mkdir dir-obj)))
