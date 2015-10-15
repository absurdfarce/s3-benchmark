(ns s3-benchmark.conf)


(def default-creds {:access-key (System/getenv "AWS_ACCESS_KEY")
                    :secret-key (System/getenv "AWS_SECRET_KEY")})

(def default-bucket (System/getenv "AWS_BUCKET"))

(def default-report-dir (System/getenv "AWS_REPORT_DIR"))