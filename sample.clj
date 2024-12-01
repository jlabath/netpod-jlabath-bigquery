#!/usr/bin/env bb

(require '[babashka.process :as process]
         '[netpod.pods :as pods])

(pods/with-pod "./netpod-jlabath-bigquery"
  (let [qry (resolve 'netpod.jlabath.bigquery/query)
        results (qry "SELECT ID,First,Last,Email,NetWorth FROM `my-project.sample_ds.contacts` LIMIT 1000")]
    (println @results)))


