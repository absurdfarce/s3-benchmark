(ns s3-benchmark.analyze
  (:require [clojure.java.io :as io]
            [s3-benchmark.conf :as conf]))

(defn ms->s [ms] (/ ms 1000M))
(defn bytes->MB [bytes] (/ bytes 1000000M))

(defn load-result-file
  [filename]
  (let [result-file (java.io.File. conf/default-report-dir filename)]
    (with-open [reader (io/reader result-file)]
      (read (java.io.PushbackReader. reader)))))


(defn transform-download-data
  [result]
  (let [{{:keys [chunk-size chunk-count]} :params [_ download-data] :transfers} result]
    (-> download-data
        (assoc :file-size (* chunk-size chunk-count))
        (dissoc :name))))


(defn compute-download-speed
  [result]
  (let [{:keys [file-size wall-time]} result]
    (print "wall-time: " wall-time ", file-size: " file-size)
    (assoc result :MB-per-sec
                  (with-precision 5 (/ (bytes->MB file-size) (ms->s wall-time))))))


(defn extract-download-speed
  [result-seq]
  (for [result result-seq]
    (-> result
        transform-download-data
        compute-download-speed
        (get :MB-per-sec)
        (.floatValue))))

(defn average
  [seq]
  (/ (reduce + seq) (count seq)))
