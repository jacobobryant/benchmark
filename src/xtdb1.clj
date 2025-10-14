(ns xtdb1
  (:require
   [clojure.java.io :as io]
   [core]
   [xtdb.api :as xt]))

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

(def benchmarks
  {:basename "xtdb1"
   :setup setup
   :with-conn (fn [f]
                (with-open [node (start-node)
                            db (xt/db node)]
                  (f db)))
   :run-query (fn [conn [f & args]]
                (apply f conn args))

   :queries
   {:get-user-by-email
    [xt/q
     '{:find [(pull user [*])]
       :in [email]
       :where [[user :user/email email]]}
     core/user-email]

    :get-user-by-id
    [xt/entity core/user-id]

    :get-user-id-by-email
    [xt/q
     '{:find [user]
       :in [email]
       :where [[user :user/email email]]}
     core/user-email]

    :get-user-email-by-id
    [xt/q
     '{:find [email]
       :in [user]
       :where [[user :user/email email]]}
     core/user-id]

    :get-feeds
    [xt/q
     '{:find [(count feed)]
       :in [user]
       :where [[sub :sub/user user]
               [sub :sub.feed/feed feed]]}
     core/user-id]

    :get-items
    [xt/q
     '{:find [(count item)]
       :in [user]
       :where [[sub :sub/user user]
               [sub :sub.feed/feed feed]
               [item :item.feed/feed feed]]}
     core/user-id]}})
