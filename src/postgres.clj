(ns postgres
  (:require
   [clojure.java.io :as io]
   [clojure.pprint :as pprint]
   [clojure.string :as str]
   [core]
   [next.jdbc :as jdbc]))

(def benchmarks
  [{:id       :get-user-by-email
    :expected nil
    :f        #(jdbc/execute! % ["select * from \"user\" where email = ?"
                                 core/user-email])
    :n        50}
   {:id       :get-user-by-id
    :expected nil
    :f        #(jdbc/execute! % ["select * from \"user\" where id = ?"
                                 core/user-id])
    :n        50}
   {:id       :get-user-id-by-email
    :expected nil
    :f        #(jdbc/execute! % ["select id from \"user\" where email = ?"
                                 core/user-email])
    :n        50}
   {:id       :get-user-email-by-id
    :expected nil
    :f        #(jdbc/execute! % ["select email from \"user\" where id = ?"
                                core/user-id])
    :n        50}
   {:id       :get-feeds
    :expected nil
    :f        #(jdbc/execute! % [(str "select count(s.feed_id) "
                                      "from sub s "
                                      "where s.user_id = ? "
                                      "and s.feed_id is not null")
                                 core/user-id])
    :n        10}
   {:id       :get-items
    :expected nil
    :f        #(jdbc/execute! % [(str "select count(i.id) "
                                      "from sub s "
                                      "join item i on i.feed_id = s.feed_id "
                                      "where s.user_id = ? "
                                      "and s.feed_id is not null")
                                 core/user-id])
    :n        10}])

(def datasource
  (jdbc/get-datasource
   {:dbtype "postgresql"
    :host "localhost"
    :port 5433
    :dbname "main"
    :user "user"
    :password "abc123"}))

(defn get-conn []
  (jdbc/get-connection datasource))

(defn create-tables []
  (with-open [conn (get-conn)]
    (doseq [schema (str/split (slurp (io/resource "postgres-schema.sql")) #";")
            :when (not-empty (str/trim schema))]
      (jdbc/execute! conn [schema]))))

(defn edn-str [x]
  (when x (pr-str x)))

(defn user-doc->db-row [user]
  {:id                 (:xt/id user)
   :email              (:user/email user)
   :roles              (edn-str (:user/roles user))
   :joined_at          (:user/joined-at user)
   :digest_days        (edn-str (:user/digest-days user))
   :digest_last_sent   (:user/digest-last-sent user)
   :from_the_sample    (:user/from-the-sample user)
   :use_original_links (:user/use-original-links user)
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
   :content_key        (:item/content-key item)
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
   :paywalled          (:item/paywalled item)
   :kind               (cond
                         (contains? item :item.feed/feed) "feed"
                         (contains? item :item.email/sub) "email"
                         (= (:item/doc-type item) :item/direct) "direct"
                         :else nil)
   :feed_id            (:item.feed/feed item)
   :guid               (:item.feed/guid item)
   :email_sub_id       (:item.email/sub item)
   :raw_content_key    (:item.email/raw-content-key item)
   :list_unsubscribe   (:item.email/list-unsubscribe item)
   :list_unsubscribe_post (:item.email/list-unsubscribe-post item)
   :reply_to           (:item.email/reply-to item)
   :maybe_confirmation (:item.email/maybe-confirmation item)
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
   :paused         (:ad/paused ad)
   :payment_failed (:ad/payment-failed ad)
   :customer_id    (:ad/customer-id ad)
   :session_id     (:ad/session-id ad)
   :payment_method (:ad/payment-method ad)
   :card_details   (edn-str (:ad/card-details ad))})

(defn ad-click-doc->db-row [ad-click]
  {:id         (:xt/id ad-click)
   :user_id    (:ad.click/user ad-click)
   :ad_id      (:ad.click/ad ad-click)
   :created_at (:ad.click/created-at ad-click)
   :cost       (:ad.click/cost ad-click)
   :source     (some-> (:ad.click/source ad-click) name)})

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

(defn convert-stuff [m]
  (into {}
        (map (fn [[k v]]
               [k (cond
                    (instance? java.time.Instant v)
                    (java.sql.Timestamp/from v)

                    (string? v)
                    (str/replace v "\u0000" "")

                    :else
                    v)]))
        m))

(defn ingest []
  (println "ingest")
  (with-open [conn (get-conn)]
    (println)
    (doseq [[dir table row-fn] [["users" "\"user\"" user-doc->db-row]
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
                :let [sql (insert table (convert-stuff (row-fn doc)))]]
          (try
            (jdbc/execute! tx sql)
            (catch Exception e
              (pprint/pprint sql)
              (pprint/pprint doc)
              (throw e))))))))


(defn setup []
  (println "setting up postgres")
  (create-tables)
  (ingest))

(defn benchmark []
  (println "benchmarking postgres")
  (with-open [conn (get-conn)]
    (core/test-benchmarks conn benchmarks) ; warm up
    (core/run-benchmarks conn benchmarks)))

(defn -main [command]
  (case command
    "setup" (setup)
    "benchmark" (benchmark))
  (System/exit 0))
