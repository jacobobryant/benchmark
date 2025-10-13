(ns sqlite
  (:require
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

(def expected-user #:user{:timezone nil,
                          :cancel_at nil,
                          :plan nil,
                          :email_username "Sn0a6",
                          :id 4399,
                          :customer_id "JIKApV1OsDDIp9uKRb",
                          :send_digest_at "08:00",
                          :joined_at nil,
                          :digest_last_sent 1759082846,
                          :roles "#{:admin}",
                          :email "w6qhyZcYmAcXOoLWrq",
                          :from_the_sample 0,
                          :suppressed_at nil,
                          :digest_days "#{:saturday :tuesday :wednesday :sunday :friday :monday :thursday}",
                          :use_original_links 0})

(def benchmarks
  (mapv
   (fn [benchmark]
     (cond-> benchmark
       (not (:f benchmark))
       (assoc :f #(jdbc/execute! % (:query benchmark)))))
   [{:id       :get-user-by-email
     :expected [expected-user]
     :query    ["select * from user where email = ?" core/user-email]}
    {:id       :get-user-by-id
     :expected [expected-user]
     :query    ["select * from user where id = ?" core/user-id-int]}
    {:id       :get-user-id-by-email
     :expected [{:user/id core/user-id-int}]
     :query    ["select id from user where email = ?" core/user-email]}
    {:id       :get-user-email-by-id
     :expected [{:user/email core/user-email}]
     :query    ["select email from user where id = ?" core/user-id-int]}
    {:id       :get-feeds
     :expected [{(keyword "count(s.feed_id)") 162}]
     :query    [(str "select count(s.feed_id) "
                     "from sub s "
                     "where s.user_id = ? "
                     "and s.feed_id is not null")
                core/user-id-int]}
    {:id       :get-items
     :expected [{(keyword "count(i.id)") 11284}]
     :query    [(str "select count(i.id) "
                     "from sub s "
                     "join item i on i.feed_id = s.feed_id "
                     "where s.user_id = ? "
                     "and s.feed_id is not null")
                core/user-id-int]}]))

(def datasource (jdbc/get-datasource {:dbtype "sqlite" :dbname "storage/db.sqlite"}))

(defn get-conn []
  (jdbc/get-connection datasource))

(defn create-tables []
  (println "creating tables")
  (with-open [conn (get-conn)]
    (doseq [schema (str/split (slurp (io/resource "sqlite-schema.sql")) #";")
            :when (not-empty (str/trim schema))]
      (jdbc/execute! conn [schema]))))

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
   :user           (:ad/user ad)
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
  (create-tables)
  (create-id-mapping)
  (ingest))

(defn benchmark []
  (println "benchmarking sqlite")
  (with-open [conn (get-conn)]
    (core/test-benchmarks conn benchmarks) ; warm up
    (core/run-benchmarks conn benchmarks)))

(defn -main [command]
  (case command
    "setup" (setup)
    "benchmark" (benchmark))
  (System/exit 0))
