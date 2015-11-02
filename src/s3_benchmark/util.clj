(ns s3-benchmark.util
  "Collection of helper functions used to make writing the test cases and benchmarking code."
  (:require [clojure.java.io :as io]
            [clojure.data.generators :as generators]
            [clojure.string :as string]
            [clojure.tools.logging :as log])
  (:import [java.util.zip CRC32]))

(defn ms->s [ms] (/ ms 1000M))
(defn bytes->MB [bytes] (/ bytes 1000000M))

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
  [tmp-dir chunk-size num-chunks]
  (let [tmp-file (io/file tmp-dir (.toString (java.util.UUID/randomUUID)))]
    (with-open [tmp-output-stream (io/output-stream tmp-file)]
      (dotimes [_ num-chunks]
        (write-random-bytes! tmp-output-stream chunk-size)))
    tmp-file))

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

(defn ensure-dir-exists
  "Creates the given directory if it doesn't already exist."
  [^java.io.File dir-obj]
  (if-not (.exists dir-obj)
    (.mkdir dir-obj)))
