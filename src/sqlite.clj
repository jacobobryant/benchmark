(ns sqlite
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.pprint :as pprint]
   [clojure.string :as str]
   [core]
   [next.jdbc :as jdbc]
   [taoensso.nippy :as nippy])
  (:import
   [java.nio ByteBuffer]
   [java.time Instant]
   [java.util UUID]))

(def datasource (jdbc/get-datasource {:dbtype "sqlite" :dbname "storage/db.sqlite"}))

(defn get-conn []
  (jdbc/get-connection datasource))

(defn update-schema []
  (println "updating schema")
  (with-open [conn (get-conn)]
    (doseq [schema (str/split (slurp (io/resource "sqlite-schema.sql")) #";")
            :when (not-empty (str/trim schema))]
      (jdbc/execute! conn [schema]))
    (when (empty? (jdbc/execute! conn ["select 1 from pragma_table_info('ad') where name = 'user_id'"]))
      (jdbc/execute! conn ["alter table ad rename user to user_id"]))))

(defn get-ids [doc]
  (mapcat (fn [[k v]]
            (cond
              (#{:xt/id
                 :sub/user
                 :sub.feed/feed
                 :item.feed/feed
                 :item.email/sub
                 :user-item/user
                 :user-item/item
                 :digest/user
                 :digest/subject
                 :digest/ad
                 :skip/user
                 :ad/user
                 :ad.click/user
                 :ad.click/ad
                 :ad.credit/ad}
               k)
              [v]

              (#{:digest/icymi
                 :digest/discover
                 :bulk-send/digests
                 :skip/items
                 :skip/clicked} k)
              v))
          doc))

(defn create-id-mapping []
  (when-not (.exists (io/file "storage/id-mapping-sqlite.nippy"))
    (println "create-id-mapping")
    (io/make-parents "storage/_")
    (nippy/freeze-to-file
     "storage/id-mapping-sqlite.nippy"
     (-> (reduce (fn [[i m] id]
                   (if (contains? m id)
                     [i m]
                     [(inc i) (assoc! m id i)]))
                 [0 (transient {})]
                 (for [{:keys [dir]} core/input-info
                       doc (core/read-docs dir)
                       id (get-ids doc)]
                   id))
         second
         persistent!))))

(defn instant->epoch [inst]
  (when inst
    (.getEpochSecond ^Instant inst)))

(defn update-ids [doc id-mapping]
  (into {}
        (map (fn [[k v]]
               [k (cond
                    (#{:xt/id
                       :sub/user
                       :sub.feed/feed
                       :item.feed/feed
                       :item.email/sub
                       :user-item/user
                       :user-item/item
                       :digest/user
                       :digest/subject
                       :digest/ad
                       :skip/user
                       :ad/user
                       :ad.click/user
                       :ad.click/ad
                       :ad.credit/ad}
                     k)
                    (id-mapping v)

                    (#{:digest/icymi :digest/discover :bulk-send/digests} k)
                    (mapv id-mapping v)

                    (#{:skip/items :skip/clicked} k)
                    (into #{} (map id-mapping) v)

                    (inst? v)
                    (instant->epoch v)

                    :else v)]))
        doc))

(defn edn-str [x]
  (when x (pr-str x)))

(defn uuid->blob [^UUID u]
  (when u
    (let [bb (ByteBuffer/allocate 16)]
      (.putLong bb (.getMostSignificantBits u))
      (.putLong bb (.getLeastSignificantBits u))
      (.array bb))))

(defn bool->int [b]
  (when (some? b) (if b 1 0)))

(defn user-doc->db-row [user]
  {:id                 (:xt/id user)
   :email              (:user/email user)
   :roles              (edn-str (:user/roles user))
   :joined_at          (:user/joined-at user)
   :digest_days        (edn-str (:user/digest-days user))
   :send_digest_at     (:user/send-digest-at user)
   :digest_last_sent   (:user/digest-last-sent user)
   :from_the_sample    (if (:user/from-the-sample user) 1 0)
   :use_original_links (if (:user/use-original-links user) 1 0)
   :suppressed_at      (:user/suppressed-at user)
   :email_username     (:user/email-username user)
   :customer_id        (:user/customer-id user)
   :plan               (some-> (:user/plan user) name)
   :cancel_at          (:user/cancel-at user)})

(defn sub-doc->db-row [sub]
  {:id              (:xt/id sub)
   :user_id         (:sub/user sub)
   :created_at      (:sub/created-at sub)
   :pinned_at       (:sub/pinned-at sub)
   :kind            (cond
                      (contains? sub :sub.feed/feed) "feed"
                      (contains? sub :sub.email/from) "email"
                      :else nil)
   :feed_id         (:sub.feed/feed sub)
   :email_from      (:sub.email/from sub)
   :unsubscribed_at (:sub.email/unsubscribed-at sub)})

(defn item-doc->db-row [item]
  {:id                 (:xt/id item)
   :ingested_at        (:item/ingested-at item)
   :title              (:item/title item)
   :url                (:item/url item)
   :redirect_urls      (edn-str (:item/redirect-urls item))
   :content            (:item/content item)
   :content_key        (uuid->blob (:item/content-key item))
   :published_at       (:item/published-at item)
   :excerpt            (:item/excerpt item)
   :author_name        (:item/author-name item)
   :author_url         (:item/author-url item)
   :feed_url           (:item/feed-url item)
   :lang               (:item/lang item)
   :site_name          (:item/site-name item)
   :byline             (:item/byline item)
   :length             (:item/length item)
   :image_url          (:item/image-url item)
   :paywalled          (bool->int (:item/paywalled item))
   :kind               (cond
                         (contains? item :item.feed/feed) "feed"
                         (contains? item :item.email/sub) "email"
                         (= (:item/doc-type item) :item/direct) "direct"
                         :else nil)
   :feed_id            (:item.feed/feed item)
   :guid               (:item.feed/guid item)
   :email_sub_id       (:item.email/sub item)
   :raw_content_key    (uuid->blob (:item.email/raw-content-key item))
   :list_unsubscribe   (:item.email/list-unsubscribe item)
   :list_unsubscribe_post (:item.email/list-unsubscribe-post item)
   :reply_to           (:item.email/reply-to item)
   :maybe_confirmation (bool->int (:item.email/maybe-confirmation item))
   :candidate_status   (some-> (:item.direct/candidate-status item) name)})

(defn feed-doc->db-row [feed]
  {:id            (:xt/id feed)
   :url           (:feed/url feed)
   :synced_at     (:feed/synced-at feed)
   :title         (:feed/title feed)
   :description   (:feed/description feed)
   :image_url     (:feed/image-url feed)
   :etag          (:feed/etag feed)
   :last_modified (:feed/last-modified feed)
   :failed_syncs  (:feed/failed-syncs feed)
   :moderation    (some-> (:feed/moderation feed) name)})

(defn user-item-doc->db-row [user-item]
  {:id             (:xt/id user-item)
   :user_id        (:user-item/user user-item)
   :item_id        (:user-item/item user-item)
   :viewed_at      (:user-item/viewed-at user-item)
   :skipped_at     (:user-item/skipped-at user-item)
   :bookmarked_at  (:user-item/bookmarked-at user-item)
   :favorited_at   (:user-item/favorited-at user-item)
   :disliked_at    (:user-item/disliked-at user-item)
   :reported_at    (:user-item/reported-at user-item)
   :report_reason  (:user-item/report-reason user-item)})

;; Skipping these because I didn't want to deal with join tables (digest_items, skip_items)
;; and the code here doesn't actually work (ai-generated)
#_(defn digest-doc->db-row [digest]
  {:id             (:xt/id digest)
   :user_id        (:digest/user digest)
   :sent_at        (:digest/sent-at digest)
   :subject_item_id (:digest/subject digest)
   :ad_id          (:digest/ad digest)
   :icymi          (edn-str (:digest/icymi digest))
   :discover       (edn-str (:digest/discover digest))
   :mailersend_id  (:digest/mailersend-id digest)})

#_(defn digest-items-doc->db-row [digest-item]
  {:id        (:xt/id digest-item)
   :digest_id (:digest-items/digest digest-item)
   :item_id   (:digest-items/item digest-item)
   :kind      (:digest-items/kind digest-item)})

#_(defn skip-doc->db-row [skip]
  {:id                 (:xt/id skip)
   :user_id            (:skip/user skip)
   :timeline_created_at (:skip/timeline-created-at skip)
   :items              (edn-str (:skip/items skip))
   :clicked            (edn-str (:skip/clicked skip))})

#_(defn skip-items-doc->db-row [skip-item]
  {:id      (:xt/id skip-item)
   :skip_id (:skip-items/skip skip-item)
   :item_id (:skip-items/item skip-item)
   :kind    (:skip-items/kind skip-item)})

(defn ad-doc->db-row [ad]
  {:id             (:xt/id ad)
   :user_id        (:ad/user ad)
   :approve_state  (some-> (:ad/approve-state ad) name)
   :updated_at     (:ad/updated-at ad)
   :balance        (:ad/balance ad)
   :recent_cost    (:ad/recent-cost ad)
   :bid            (:ad/bid ad)
   :budget         (:ad/budget ad)
   :url            (:ad/url ad)
   :title          (:ad/title ad)
   :description    (:ad/description ad)
   :image_url      (:ad/image-url ad)
   :paused         (bool->int (:ad/paused ad))
   :payment_failed (bool->int (:ad/payment-failed ad))
   :customer_id    (:ad/customer-id ad)
   :session_id     (:ad/session-id ad)
   :payment_method (:ad/payment-method ad)
   :card_details   (edn-str (:ad/card-details ad))})

(defn ad-click-doc->db-row [ad-click]
  {:id        (:xt/id ad-click)
   :user_id   (:ad.click/user ad-click)
   :ad_id     (:ad.click/ad ad-click)
   :created_at (:ad.click/created-at ad-click)
   :cost      (:ad.click/cost ad-click)
   :source    (some-> (:ad.click/source ad-click) name)})

(defn ad-credit-doc->db-row [ad-credit]
  {:id            (:xt/id ad-credit)
   :ad_id         (:ad.credit/ad ad-credit)
   :source        (some-> (:ad.credit/source ad-credit) name)
   :amount        (:ad.credit/amount ad-credit)
   :created_at    (:ad.credit/created-at ad-credit)
   :charge_status (some-> (:ad.credit/charge-status ad-credit) name)})

(defn insert [table doc]
  (into [(str "INSERT INTO " table " (" (str/join ", " (mapv name (keys doc)))
              ") VALUES (" (str/join ", " (repeat (count doc) "?")) ")")]
        (vals doc)))

(defn ingest []
  (println "ingest")
  (let [id-mapping (nippy/thaw-from-file "storage/id-mapping-sqlite.nippy")]
    (with-open [conn (get-conn)]
      (println)
      (doseq [[dir table row-fn] [["users" "user" user-doc->db-row]
                                  ["subs" "sub" sub-doc->db-row]
                                  ["feeds" "feed" feed-doc->db-row]
                                  ["items" "item" item-doc->db-row]
                                  ["user-items" "user_item" user-item-doc->db-row]
                                  ["ads" "ad" ad-doc->db-row]
                                  ["ad-clicks" "ad_click" ad-click-doc->db-row]
                                  ["ad-credits" "ad_credit" ad-credit-doc->db-row]]
              [i batch] (map-indexed vector (partition-all 1000 (core/read-docs dir)))]
        (printf "\r  %s batch %d" dir i)
        (flush)
        (jdbc/with-transaction [tx conn]
          (doseq [doc batch
                  :let [sql-doc (row-fn (update-ids doc id-mapping))
                        sql (insert table sql-doc)]]
            (try
              (jdbc/execute! tx sql)
              (catch Exception e
                (pprint/pprint sql)
                (pprint/pprint doc)
                (throw e)))))))))

(defn setup []
  (println "setting up sqlite")
  (update-schema)
  (create-id-mapping)
  (ingest))

(def benchmarks
  {:basename "sqlite"
   :migrate update-schema
   :with-conn (fn [f]
                (with-open [conn (get-conn)]
                  (f conn)))
   :run-query jdbc/execute!
   :setup setup
   :queries
   {:get-user-by-email
    ["select * from user where email = ?" core/user-email]

    :get-user-by-id
    ["select * from user where id = ?" core/user-id-int]

    :get-user-id-by-email
    ["select id from user where email = ?" core/user-email]

    :get-user-email-by-id
    ["select email from user where id = ?" core/user-id-int]

    :get-feeds
    ["select count(s.feed_id)
      from sub s
      where s.user_id = ?
      and s.feed_id is not null"
     core/user-id-int]

    :get-items
    ["select count(i.id)
      from sub s
      join item i on i.feed_id = s.feed_id
      where s.user_id = ?
      and s.feed_id is not null"
     core/user-id-int]

    ;; model/recommend.clj
    :read-urls
    ["select distinct item.url
      from user_item
      join item on item.id = user_item.item_id
      where user_item.user_id = ?
      and coalesce(viewed_at, favorited_at,  disliked_at, reported_at) is not null
      and item.url is not null"
     core/user-id-int]

    :email-items
    ["select sub.id, item.id from sub
      join item on sub.id = item.email_sub_id
      where sub.user_id = ?"
     core/user-id-int]

    :rss-items
    ["select sub.id, item.id
      from sub
      join item on item.feed_id = sub.feed_id
      where sub.user_id = ?
      and sub.feed_id is not null"
     core/user-id-int]

    :user-items
    ["select item_id, viewed_at, favorited_at, disliked_at, reported_at
      from user_item
      where user_id = ?"
     core/user-id-int]

    ;; work.subscription
    :active-users-by-joined-at
    ["select id from user where joined_at > ?"
     (quot (inst-ms #inst "2025") 1000)]

    :active-users-by-viewed-at
    ["select distinct user_item.user_id from user_item where viewed_at > ?"
     (quot (inst-ms #inst "2025") 1000)]

    :active-users-by-ad-updated
    ["select user_id from ad where updated_at > ?"
     (quot (inst-ms #inst "2025") 1000)]

    :active-users-by-ad-clicked
    ["select distinct user_id from ad_click where created_at > ?"
     (quot (inst-ms #inst "2025") 1000)]

    :feeds-to-sync
    (let [{user-ids :ints} (core/read-fixture "active-user-ids.edn")]
      (concat [(str "select distinct sub.feed_id
                     from sub
                     join feed on feed.id = sub.feed_id
                     where sub.user_id in " (core/?s (count user-ids))
                    " and (feed.synced_at is null or feed.synced_at < ?)")]
              user-ids
              [(- (quot (inst-ms core/latest-t) 1000)
                  (* 60 60 2))]))

    :existing-feed-titles
    (let [{:keys [id-int real-titles random-titles]} (core/read-fixture "biggest-feed.edn")
          titles (concat real-titles random-titles)]
      (concat [(str "select title
                     from item
                     where item.feed_id = ?
                     and title in " (core/?s (count titles)))
               id-int]
              titles))

    :favorited-urls
    ["select url
      from item
      join user_item on user_item.item_id = item.id
      where user_item.favorited_at is not null"]

    :direct-urls
    ["select url from item where kind = 'direct'"]

    :recent-email-items
    ["select item.id, ingested_at
      from sub
      join item on sub.id = item.email_sub_id
      where sub.user_id = ?
      and item.ingested_at > ?"
     core/user-id-int
     (quot (inst-ms #inst "2025-09-15T05:36:23Z") 1000)]

    :recent-rss-items
    ["select item.id, ingested_at
      from sub
      join item on item.feed_id = sub.feed_id
      where sub.user_id = ?
      and sub.feed_id is not null
      and item.ingested_at > ?"
     core/user-id-int
     (quot (inst-ms #inst "2025-09-15T05:36:23Z") 1000)]

    :recent-bookmarks
    ["select item_id, bookmarked_at
      from user_item
      where user_id = ?
      and bookmarked_at > ?"
     core/user-id-int
     (quot (inst-ms #inst "2024-09-15T05:36:23Z") 1000)]

    :subscription-status
    ["select id, unsubscribed_at
      from sub
      where sub.user_id = ?"
     core/user-id-int]

    :latest-emails-received-at
    ["select sub.id, max(item.ingested_at)
      from sub
      join item on item.email_sub_id = sub.id
      where sub.user_id = ?
      group by sub.id"
     core/user-id-int]

    :unique-ad-clicks
    ["select ad_id, count(distinct user_id)
      from ad_click
      group by ad_id"]

    :latest-ad-clicks
    ["select ad_id, max(created_at)
      from ad_click
      group by ad_id"]

    :charge-amounts-by-status
    ["select ad_id, charge_status, sum(amount)
      from ad_credit
      where charge_status is not null
      group by ad_id, charge_status"]

    :candidate-statuses
    ["select candidate_status, count(id)
      from item
      where candidate_status is not null
      group by candidate_status"]

    :feed-sub-urls
    ["select feed.url
      from feed
      join sub on sub.feed_id = feed.id
      where sub.user_id = ?"
     core/user-id-int]

    :favorites
    ["select user_id, item_id
      from user_item
      where favorited_at is not null"]

    :approved-candidates
    ["select id, url
      from item
      where candidate_status = 'approved'"]

    :ad-recent-cost
    ["select ad_id, sum(cost)
      from ad_click
      where created_at > ?
      group by ad_id"
     (quot (inst-ms #inst "2025") 1000)]

    :ads-clicked-at
    ["select ad_id, user_id, max(created_at)
      from ad_click
      group by ad_id, user_id"]

    :all-n-likes
    ["select item_id, count(id)
      from user_item
      where favorited_at is not null
      group by item_id"]}})

(defn q [& query]
  (jdbc/execute! (get-conn) (vec query)))

(defn q-benchmark [id]
  (apply q (get-in benchmarks [:queries id])))

(def int-id->uuid
  (delay (into {}
               (map (comp vec reverse))
               (nippy/thaw-from-file "storage/id-mapping-sqlite.nippy"))))


(comment

  (q "select distinct charge_status from ad_credit")

  (count (time (q-benchmark :all-n-likes)))
  )

(defn create-fixtures []
  (let [t (- (quot (inst-ms core/latest-t) 1000)
             (* 60 60 24 30 6))
        user-ids (->> (concat (q "select id from user where joined_at > ?"
                                 (quot (inst-ms #inst "2025") 1000))
                              (q "select user_item.user_id from user_item where viewed_at > ?"
                                 (quot (inst-ms #inst "2025") 1000))
                              (q "select user_id from ad where updated_at > ?"
                                 (quot (inst-ms #inst "2025") 1000)))
                      (mapv (comp val first))
                      distinct
                      vec)]
    (spit "resources/active-user-ids.edn"
          (with-out-str
           (pprint/pprint
            {:ints user-ids
             :uuids (mapv @int-id->uuid user-ids)}))))

  (let [biggest-feed (-> (q "select feed.id, count(item.id) as n_items
                             from feed join item on item.feed_id = feed.id
                             group by feed.id
                             order by n_items desc
                             limit 1")
                         first
                         :feed/id)
        item-titles (->> (q "select title from item
                             where feed_id = ?
                             order by random()
                             limit 10
                             "
                            biggest-feed)
                         (mapv :item/title))]
    (spit "resources/biggest-feed.edn"
          (with-out-str
           (pprint/pprint
            {:id-int biggest-feed
             :id-uuid (get @int-id->uuid biggest-feed)
             :real-titles item-titles
             :random-titles (repeatedly 10 #(core/random-string (+ 5 (rand-int 15))))})))))

