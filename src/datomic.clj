(ns datomic
  (:require
   [clojure.pprint :refer [pprint]]
   [clojure.set :as set]
   [clojure.walk :as walk]
   [core]
   [datomic.client.api :as d]
   [taoensso.nippy :as nippy])
  (:import
   [java.time Instant]
   [java.util UUID Date]))

(def client
  (d/client {:server-type :datomic-local
             :system "yakread-benchmark"}))

(defn conn []
  (d/connect client {:db-name "yakread-benchmark"}))

(defn attribute [ident value-type & args]
  (let [opts (first (filterv map? args))
        flags (set (remove map? args))]
    (merge {:db/ident      ident
            :db/valueType  value-type
            :db/cardinality (get flags :db.cardinality/many :db.cardinality/one)}
           (when (flags :db.unique/identity)
             {:db/unique :db.unique/identity})
           opts)))

(defn composite [ident & attrs]
  {:db/ident ident
   :db/valueType :db.type/tuple
   :db/tupleAttrs (vec attrs)
   :db/cardinality :db.cardinality/one
   :db/unique :db.unique/identity})

(defn squuid
  "like datomic's squuid but takes instant as a param"
  [^Instant instant]
  (let [epoch-seconds (.getEpochSecond instant)
        msb (bit-shift-left epoch-seconds 32)
        lsb (.nextLong (java.util.Random.))]
    (java.util.UUID. msb lsb)))

(def user-attrs
  [(attribute :user/id                 :db.type/uuid    :db.unique/identity)
   (attribute :user/email              :db.type/string  :db.unique/identity)
   (attribute :user/roles              :db.type/keyword :db.cardinality/many)
   (attribute :user/joined-at          :db.type/instant)
   (attribute :user/digest-days        :db.type/keyword :db.cardinality/many)
   (attribute :user/send-digest-at     :db.type/string)
   (attribute :user/timezone           :db.type/string)
   (attribute :user/digest-last-sent   :db.type/instant)
   (attribute :user/from-the-sample    :db.type/boolean)
   (attribute :user/use-original-links :db.type/boolean)
   (attribute :user/suppressed-at      :db.type/instant)
   (attribute :user/email-username     :db.type/string :db.unique/identity)
   (attribute :user/customer-id        :db.type/string)
   (attribute :user/plan               :db.type/keyword)
   (attribute :user/cancel-at          :db.type/instant)])

(def sub-attrs
  [(attribute :sub/id                    :db.type/uuid :db.unique/identity)
   (attribute :sub/user                  :db.type/ref)
   (attribute :sub/created-at            :db.type/instant)
   (attribute :sub/pinned-at             :db.type/instant)
   (attribute :sub.feed/feed             :db.type/ref)
   (attribute :sub.email/from            :db.type/string)
   (attribute :sub.email/unsubscribed-at :db.type/instant)])

(def feed-attrs
  [(attribute :feed/id            :db.type/uuid    :db.unique/identity)
   (attribute :feed/url           :db.type/string)
   (attribute :feed/synced-at     :db.type/instant)
   (attribute :feed/title         :db.type/string)
   (attribute :feed/description   :db.type/string)
   (attribute :feed/image-url     :db.type/string)
   (attribute :feed/etag          :db.type/string)
   (attribute :feed/last-modified :db.type/string)
   (attribute :feed/failed-syncs  :db.type/long)
   (attribute :feed/moderation    :db.type/keyword)])

(def item-attrs
  [(attribute :item/id                          :db.type/uuid    :db.unique/identity)
   (attribute :item/ingested-at                 :db.type/instant)
   (attribute :item/title                       :db.type/string)
   (attribute :item/url                         :db.type/string)
   (attribute :item/redirect-urls               :db.type/string :db.cardinality/many)
   (attribute :item/content                     :db.type/string)
   (attribute :item/content-key                 :db.type/uuid)
   (attribute :item/published-at                :db.type/instant)
   (attribute :item/excerpt                     :db.type/string)
   (attribute :item/author-name                 :db.type/string)
   (attribute :item/author-url                  :db.type/string)
   (attribute :item/feed-url                    :db.type/string)
   (attribute :item/lang                        :db.type/string)
   (attribute :item/site-name                   :db.type/string)
   (attribute :item/byline                      :db.type/string)
   (attribute :item/length                      :db.type/long)
   (attribute :item/image-url                   :db.type/string)
   (attribute :item/paywalled                   :db.type/boolean)
   (attribute :item.feed/feed                   :db.type/ref)
   (attribute :item.feed/guid                   :db.type/string)
   (attribute :item.email/sub                   :db.type/ref)
   (attribute :item.email/raw-content-key       :db.type/uuid)
   (attribute :item.email/list-unsubscribe      :db.type/string)
   (attribute :item.email/list-unsubscribe-post :db.type/string)
   (attribute :item.email/reply-to              :db.type/string)
   (attribute :item.email/maybe-confirmation    :db.type/boolean)
   (attribute :item/doc-type                    :db.type/keyword)
   (attribute :item.direct/candidate-status     :db.type/keyword)])

(def user-item-attrs
  [(attribute :user-item/id            :db.type/uuid    :db.unique/identity)
   (attribute :user-item/user          :db.type/ref)
   (attribute :user-item/item          :db.type/ref)
   (attribute :user-item/viewed-at     :db.type/instant)
   (attribute :user-item/skipped-at    :db.type/instant)
   (attribute :user-item/bookmarked-at :db.type/instant)
   (attribute :user-item/favorited-at  :db.type/instant)
   (attribute :user-item/disliked-at   :db.type/instant)
   (attribute :user-item/reported-at   :db.type/instant)
   (attribute :user-item/report-reason :db.type/string )
   (composite :user-ite/user+item      :user-item/user  :user-item/item)])

(def ad-attrs
  [(attribute :ad/id             :db.type/uuid    :db.unique/identity)
   (attribute :ad/user           :db.type/ref)
   (attribute :ad/approve-state  :db.type/keyword)
   (attribute :ad/updated-at     :db.type/instant)
   (attribute :ad/balance        :db.type/long)
   (attribute :ad/recent-cost    :db.type/long)
   (attribute :ad/bid            :db.type/long)
   (attribute :ad/budget         :db.type/long)
   (attribute :ad/url            :db.type/string)
   (attribute :ad/title          :db.type/string)
   (attribute :ad/description    :db.type/string)
   (attribute :ad/image-url      :db.type/string)
   (attribute :ad/paused         :db.type/boolean)
   (attribute :ad/payment-failed :db.type/boolean)
   (attribute :ad/customer-id    :db.type/string)
   (attribute :ad/session-id     :db.type/string)
   (attribute :ad/payment-method :db.type/string)
   (attribute :ad/card-details   :db.type/string)])

(def ad-click-attrs
  [(attribute :ad.click/id          :db.type/uuid    :db.unique/identity)
   (attribute :ad.click/user        :db.type/ref)
   (attribute :ad.click/ad          :db.type/ref)
   (attribute :ad.click/created-at  :db.type/instant)
   (attribute :ad.click/cost        :db.type/long)
   (attribute :ad.click/source      :db.type/keyword)])

(def ad-credit-attrs
  [(attribute :ad.credit/id            :db.type/uuid    :db.unique/identity)
   (attribute :ad.credit/ad            :db.type/ref)
   (attribute :ad.credit/source        :db.type/keyword)
   (attribute :ad.credit/amount        :db.type/long)
   (attribute :ad.credit/created-at    :db.type/instant)
   (attribute :ad.credit/charge-status :db.type/keyword)])

(def schema-tx
  (concat user-attrs
          sub-attrs
          feed-attrs
          item-attrs
          user-item-attrs
          ad-attrs
          ad-click-attrs
          ad-credit-attrs))

(def ref-mapping
  {:sub/user       :user/id
   :sub.feed/feed  :feed/id
   :item.feed/feed :feed/id
   :item.email/sub :sub/id
   :user-item/user :user/id
   :user-item/item :item/id
   :ad/user        :user/id
   :ad.click/user  :user/id
   :ad.click/ad    :ad/id
   :ad.credit/ad   :ad/id})

;; To add schema to your database:
(defn ensure-db!
  []
  (d/create-database client {:db-name "yakread-benchmark"})
  (d/transact (conn) {:tx-data schema-tx}))

(defn map-refs [doc]
  (reduce (fn [doc [ref-attr ref-target]]
            (cond-> doc
              (contains? doc ref-attr)
              (update ref-attr #(vector ref-target %))))
          doc
          ref-mapping))

(defn convert-instants [doc]
  (update-vals doc
               (fn [x]
                 (if (inst? x)
                   (Date/from x)
                   x))))

(defn convert-stuff [doc]
  (reduce (fn [doc [k f]]
            (if (contains? doc k)
              (update doc k f)
              doc))
          doc
          {:user/send-digest-at str}))

(defn truncate-strings [doc]
  (update-vals doc
               (fn [x]
                 (if (and (string? x) (< 4000 (count x)))
                   (subs x 0 4000)
                   x))))

(defn ensure-users! []
  (let [id-mapping (nippy/thaw-from-file "id-mapping.nippy")
        user-attrs [:ad/user :user-item/user :sub/user :ad.click/user]
        user-ids (time
                  (into #{}
                        (comp (map :dir)
                              (mapcat core/read-docs)
                              (mapcat #(vals (select-keys % user-attrs))))
                        core/input-info))
        user-ids-in-db (set (mapv first (d/q '[:find ?user-id
                                               :where
                                               [?user :user/id ?user-id]]
                                             (d/db (conn)))))
        missing-user-ids (set/difference user-ids user-ids-in-db)]
    (d/transact (conn)
                {:tx-data (vec
                           (for [id missing-user-ids]
                             {:user/id (get id-mapping id id)}))})))

(defn ingest []
  (let [c (conn)
        id-mapping (nippy/thaw-from-file "id-mapping.nippy")]
    (doseq [{:keys [attr dir]} (drop 3 core/input-info)
            :let [id-attr (keyword (namespace attr) "id")]
            [i batch] (->> (core/read-docs dir)
                           (map (fn [doc]
                                  (-> (walk/postwalk #(get id-mapping % %) doc)
                                      map-refs
                                      convert-instants
                                      truncate-strings
                                      convert-stuff
                                      (set/rename-keys {:xt/id id-attr})
                                      (dissoc :user/timezone))))
                           (partition-all 1000)
                           (map-indexed vector))]
      (prn dir i)
      (try
        (d/transact c {:tx-data (vec batch)})
        (catch Exception e
          (def batch* batch)
          (throw e))))))

(comment
  (ensure-db!)
  (ensure-users!)
  (ingest)

  (->> batch*
       (mapcat vals)
       (filterv string?)
       (mapv count)
       frequencies
       sort
       )

  (def conn (d/connect client {:db-name "yakread-benchmark"}))

  (->> (d/q '[:find (pull ?user [*])
              :where
              [?user :user/email]
              ]
            (d/db (conn))
            )
       (filterv (comp #{#uuid "2689f412-2a87-4180-acd2-614b001b60bd"} :user/id))
       )

  (d/pull (d/db (conn)) '[*] [:])

  (.stop conn)

  (ns-publics 'datomic.client.api)
   
  )
