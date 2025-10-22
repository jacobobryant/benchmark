(ns xtdb1
  (:require
   [clojure.java.io :as io]
   [clojure.pprint :refer [pprint]]
   [core]
   [xtdb.api :as xt])
  (:import
   [java.time Instant]))

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
     core/user-id]

    :read-urls
    [xt/q
     '{:find [url]
       :in [user]
       :where [[usit :user-item/user user]
               [usit :user-item/item item]
               [item :item/url url]
               (or [usit :user-item/viewed-at _]
                   [usit :user-item/favorited-at _]
                   [usit :user-item/disliked-at _]
                   [usit :user-item/reported-at _])]}
     core/user-id]

    :email-items
    [xt/q
     '{:find [sub item]
       :in [user]
       :where [[sub :sub/user user]
               [item :item.email/sub sub]]}
     core/user-id]

    :rss-items
    [xt/q
     '{:find [sub item]
       :in [user]
       :where [[sub :sub/user user]
               [sub :sub.feed/feed feed]
               [item :item.feed/feed feed]]}
     core/user-id]

    :user-items
    [xt/q
     '{:find [(pull usit [:user-item/item
                          :user-item/viewed-at
                          :user-item/favorited-at
                          :user-item/disliked-at
                          :user-item/reported-at])]
       :in [user]
       :where [[usit :user-item/user user]]}
     core/user-id]

    :active-users-by-joined-at
    [xt/q
     '{:find [user]
       :in [t0]
       :where [[user :user/joined-at t]
               [(< t0 t)]]}
     (.toInstant #inst "2025")]

    :active-users-by-viewed-at
    [xt/q
     '{:find [user]
       :in [t0]
       :where [[usit :user-item/user user]
               [usit :user-item/viewed-at t]
               [(< t0 t)]]}
     (.toInstant #inst "2025")]

    :active-users-by-ad-updated
    [xt/q
     '{:find [user]
       :in [t0]
       :where [[ad :ad/user user]
               [ad :ad/updated-at t]
               [(< t0 t)]]}
     (.toInstant #inst "2025")]

    :active-users-by-ad-clicked
    [xt/q
     '{:find [user]
       :in [t0]
       :where [[click :ad.click/user user]
               [click :ad.click/created-at t]
               [(< t0 t)]]}
     (.toInstant #inst "2025")]

    :feeds-to-sync
    (let [{user-ids :uuids} (core/read-fixture "active-user-ids.edn")]
      [xt/q
       {:find '[feed]
        :in '[[user ...] t0]
        :where ['[sub :sub/user user]
                '[sub :sub.feed/feed feed]
                '[feed :feed/url]
                [(list 'get-attr 'feed :feed/synced-at (Instant/ofEpochMilli 0))
                 '[synced-at ...]]
                '[(< synced-at t0)]]}
       user-ids
       (.minusSeconds (.toInstant core/latest-t)
                      (* 60 60 2))])

    :existing-feed-titles
    (let [{:keys [id-uuid real-titles random-titles]} (core/read-fixture "biggest-feed.edn")
          titles (concat real-titles random-titles)]
      [xt/q
       '{:find [title]
         :in [feed [title ...]]
         :where [[item :item.feed/feed feed]
                 [item :item/title title]]}
       id-uuid
       titles])

    :skips
    [xt/q
     '{:find [item t]
       :in [user]
       :where [[skip :skip/user user]
               [skip :skip/items item]
               [skip :skip/timeline-created-at t]]}
     core/user-id]

    :favorited-urls
    [xt/q
     '{:find [url]
       :where [[usit :user-item/item item]
               [usit :user-item/favorited-at]
               [item :item/url url]]}]

    :direct-urls
    [xt/q
     '{:find [url]
       :in [direct]
       :where [[item :item/url url]
               [item :item/doc-type direct]]}
     :item/direct]

    :recent-email-items
    [xt/q
     '{:find [item ingested-at]
       :in [user t0]
       :where [[sub :sub/user user]
               [item :item.email/sub sub]
               [item :item/ingested-at ingested-at]
               [(< t0 ingested-at)]]}
     core/user-id
     (.toInstant #inst "2025-09-15T05:36:23Z")]

    :recent-rss-items
    [xt/q
     '{:find [item ingested-at]
       :in [user t0]
       :where [[sub :sub/user user]
               [sub :sub.feed/feed feed]
               [item :item.feed/feed feed]
               [item :item/ingested-at ingested-at]
               [(< t0 ingested-at)]]}
     core/user-id
     (.toInstant #inst "2025-09-15T05:36:23Z")]

    :recent-bookmarks
    [xt/q
     '{:find [item bookmarked-at]
       :in [user t0]
       :where [[usit :user-item/user user]
               [usit :user-item/item item]
               [usit :user-item/bookmarked-at bookmarked-at]
               [(< t0 bookmarked-at)]]}
     core/user-id
     (.toInstant #inst "2024-09-15T05:36:23Z")]

    :subscription-status
    [xt/q
     '{:find [(pull sub [:xt/id :sub.email/unsubscribed-at])]
       :in [user]
       :where [[sub :sub/user user]]}
     core/user-id]

    :latest-emails-received-at
    [xt/q
     '{:find [sub (max t)]
       :in [user]
       :where [[sub :sub/user user]
               [item :item.email/sub sub]
               [item :item/ingested-at t]]}
     core/user-id]

    :unique-ad-clicks
    [xt/q
     '{:find [ad (count-distinct user)]
       :where [[click :ad.click/ad ad]
               [click :ad.click/user user]]}]

    :latest-ad-clicks
    [xt/q
     '{:find [ad (max t)]
       :where [[click :ad.click/ad ad]
               [click :ad.click/created-at t]]}]

    :charge-amounts-by-status
    [xt/q
     '{:find [ad charge-status (sum amount)]
       :where [[charge :ad.credit/ad ad]
               [charge :ad.credit/charge-status charge-status]
               [charge :ad.credit/amount amount]]}]

    :candidate-statuses
    [xt/q
     '{:find [status (count item)]
       :where [[item :item.direct/candidate-status status]]}]

    :feed-sub-urls
    [xt/q
     '{:find [url]
       :in [user]
       :where [[sub :sub/user user]
               [sub :sub.feed/feed feed]
               [feed :feed/url url]]}
     core/user-id]

    :favorites
    [xt/q
     '{:find [user item]
       :where [[usit :user-item/user user]
               [usit :user-item/item item]
               [usit :user-item/favorited-at]]}]

    :approved-candidates
    [xt/q
     '{:find [(pull item [:xt/id :item/url])]
       :where [[item :item.direct/candidate-status :approved]]}]

    :ad-recent-cost
    [xt/q
     '{:find [ad (sum cost)]
       :in [t]
       :where [[click :ad.click/ad ad]
               [click :ad.click/cost cost]
               [click :ad.click/created-at created-at]
               [(< t created-at)]]}
     (.toInstant #inst "2025")]

    :ads-clicked-at
    [xt/q
     '{:find [ad user (max t)]
       :where [[click :ad.click/user user]
               [click :ad.click/ad ad]
               [click :ad.click/created-at t]]}]

    :all-n-likes
    [xt/q
     '{:find [item (count usit)]
       :where [[usit :user-item/item item]
               [usit :user-item/favorited-at]]}]}})

(defonce node nil)

(defn q [f & query]
  (apply f (xt/db node) query))

(defn q-benchmark [id]
  (apply q (get-in benchmarks [:queries id])))

(comment
  (def node (start-node))
  (def db (xt/db node))
  (.close node)

  (count (time (q-benchmark :all-n-likes)))
  )
