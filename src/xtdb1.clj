(ns xtdb1
  (:require
   [clojure.java.io :as io]
   [core]
   [xtdb.api :as xt]))

(def expected-user
  {:user/email-username "Sn0a6"
   :user/digest-last-sent (java.time.Instant/parse "2025-09-28T18:07:26.604435Z")
   :user/send-digest-at (java.time.LocalTime/parse "08:00")
   :user/roles #{:admin}
   :user/customer-id "JIKApV1OsDDIp9uKRb"
   :user/email "w6qhyZcYmAcXOoLWrq"
   :user/digest-days #{:saturday :tuesday :wednesday :sunday :friday :monday :thursday}
   :xt/id #uuid "e86e5e14-0001-46eb-9d11-134162ce930f"
   :user/use-original-links false})

(def benchmarks
  [{:id       :get-user-by-email
    :expected #{[expected-user]}
    :f        #(xt/q %
                     '{:find [(pull user [*])]
                       :in [email]
                       :where [[user :user/email email]]}
                     core/user-email)
    :n        50}
   {:id       :get-user-by-id
    :expected #{[expected-user]}
    :f        #(xt/entity % core/user-id)
    :n        50}
   {:id       :get-user-id-by-email
    :expected #{[core/user-id]}
    :f        #(xt/q % '{:find [user]
                         :in [email]
                         :where [[user :user/email email]]}
                     core/user-email)
    :n        50}
   {:id       :get-user-email-by-id
    :expected #{[core/user-email]}
    :f        #(xt/q % '{:find [email]
                         :in [user]
                         :where [[user :user/email email]]}
                     core/user-id)
    :n        50}
   {:id       :get-feeds
    :expected #{[162]}
    :f        #(xt/q %
                     '{:find [(count feed)]
                       :in [user]
                       :where [[sub :sub/user user]
                               [sub :sub.feed/feed feed]]}
                     core/user-id)
    :n        10}
   {:id       :get-items
    :expected #{[11284]}
    :f        #(xt/q %
                     '{:find [(count item)]
                       :in [user]
                       :where [[sub :sub/user user]
                               [sub :sub.feed/feed feed]
                               [item :item.feed/feed feed]]}
                     core/user-id)
    :n        10}])

(defn start-node []
  (let [kv-store-fn (fn [basename]
                      {:kv-store {;; might be interesting to benchmark lmdb too, but eh
                                  :xtdb/module 'xtdb.rocksdb/->kv-store
                                  :db-dir (io/file "storage/xtdb1" basename)}})]
    (xt/start-node
     {:xtdb/index-store    (kv-store-fn "index")
      :xtdb/document-store (kv-store-fn "docs")
      :xtdb/tx-log         (kv-store-fn "tx-log")})))

(defn setup []
  (println "setting up XTDB 1")
  (println "ingest")
  (with-open [node (start-node)]
    (println)
    (doseq [{:keys [dir]} core/input-info
            [i batch] (->> (core/read-docs dir)
                           (partition-all 1000)
                           (map-indexed vector))]
      (printf "\r  %s batch %d" dir i)
      (flush)
      (xt/submit-tx node (for [doc batch]
                           [::xt/put doc])))
    (println "  waiting for indexing to finish...")
    (xt/sync node)))

(defn benchmark []
  (println "benchmarking XTDB 1")
  (with-open [node (start-node)
              db (xt/open-db node)]
    (core/test-benchmarks db benchmarks) ; warm up
    (core/run-benchmarks db benchmarks)))

(defn -main [command]
  (case command
    "setup" (setup)
    "benchmark" (benchmark))
  (System/exit 0))
