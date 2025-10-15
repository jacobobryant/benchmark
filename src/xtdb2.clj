(ns xtdb2
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.walk :as walk]
   [core]
   [taoensso.nippy :as nippy]
   [xtdb.api :as xt]
   [xtdb.node :as xtn]) 
  (:import
   [java.sql Timestamp]))

(defn start-node []
  (xtn/start-node
   {:log [:local {:path "storage/xtdb2/log"}]
    :storage [:local {:path "storage/xtdb2/storage"}]}))

(defn get-conn [node]
  (.build (.createConnectionBuilder node)))

(defn uuid-with-prefix
  "Returns a new UUID with the prefix group from uuid-prefix and the rest from uuid-rest."
  [uuid-prefix uuid-rest]
  (let [prefix-str (str uuid-prefix)
        rest-str   (str uuid-rest)
        prefix     (first (str/split prefix-str #"-"))
        rest-groups (rest (str/split rest-str #"-"))]
    (java.util.UUID/fromString
      (str/join "-" (cons prefix rest-groups)))))

(defn create-id-mapping
  "Creates a map of <old uuid> -> <new uuid> which will be used by ingest to assign :xt/id values
   with better locality."
  []
  (when-not (.exists (io/file "storage/id-mapping-xtdb2.nippy"))
    (println "create-id-mapping")
    (let [mapping (into {} (concat
                            (for [{:keys [xt/id sub/user]} (core/read-docs "subs")]
                              [id (uuid-with-prefix user id)])
                            (for [{:keys [xt/id user-item/user]} (core/read-docs "user-items")]
                              [id (uuid-with-prefix user id)])))
          mapping (into mapping
                        (map (fn [{:keys [xt/id item.feed/feed item.email/sub]}]
                               [id (uuid-with-prefix
                                    (or feed sub #uuid "00000000-0000-0000-0000-000000000000")
                                    id)]))
                        (core/read-docs "items"))]
      (println "  saving to storage/id-mapping-xtdb2.nippy")
      (io/make-parents "storage/_")
      (nippy/freeze-to-file "storage/id-mapping-xtdb2.nippy" mapping))))

(defn ingest []
  (println "ingest")
  (let [id-mapping (nippy/thaw-from-file "storage/id-mapping-xtdb2.nippy")]
    (with-open [node (start-node)
                conn (get-conn node)]
      (println)
      (doseq [{:keys [dir]} core/input-info
              [i batch] (->> (core/read-docs dir)
                             (partition-all 1000)
                             (map-indexed vector))
              :let [table (keyword (str/replace dir "-" "_"))
                    batch (mapv (fn [record]
                                  (-> (walk/postwalk #(get id-mapping % %) record)
                                      (dissoc :user/timezone)))
                                batch)]]
        (printf "\r  %s batch %d" dir i)
        (flush)
        (xt/submit-tx conn [(into [:put-docs table] batch)]))
      (println "\n  waiting for indexing to finish...")
      ;; Ideally would poll xtn/status
      (xt/execute-tx conn [[:put-docs "foo" {:xt/id 1}]]))))

(defn setup []
  (println "setting up XTDB 2")
  (create-id-mapping)
  (ingest))

(def benchmarks
  {:basename "xtdb2"
   :setup setup
   :with-conn (fn [f]
                (with-open [node (start-node)
                            conn (get-conn node)]
                  (f conn)))
   :run-query xt/q
   :queries
   {:get-user-by-email
    ["select * from users where user$email = ?" core/user-email]

    :get-user-by-id
    ["select * from users where _id = ?" core/user-id]

    :get-user-id-by-email
    ["select _id from users where user$email = ?" core/user-email]

    :get-user-email-by-id
    ["select user$email from users where _id = ?" core/user-id]

    :get-feeds
    [(str "select count(sub$feed$feed) from subs "
          "where sub$user = ? and sub$feed$feed is not null")
     core/user-id]

    :get-items
    [(str "select count(i._id) "
          "from subs s "
          "join items i on i.item$feed$feed = s.sub$feed$feed "
          "where s.sub$user = ? "
          "and s.sub$feed$feed is not null")
     core/user-id]

    :read-urls
    ["select distinct item$url
      from user_items
      join items on items._id = user_item$item
      where user_item$user = ?
      and coalesce(user_item$viewed_at, user_item$favorited_at, user_item$disliked_at,
      user_item$reported_at) is not null
      and item$url is not null"
     core/user-id]

    :email-items
    ["select subs._id as sub_id, items._id as item_id
      from subs
      join items on subs._id = item$email$sub
      where sub$user = ?"
     core/user-id]


    :rss-items
    ["select subs._id as sub_id, item._id as item_id
      from subs
      join items on item$feed$feed = sub$feed$feed
      where sub$user = ?"
     core/user-id]

    :user-items
    ["select user_item$item, user_item$viewed_at, user_item$favorited_at,
      user_item$disliked_at, user_item$reported_at
      from user_items
      where user_item$user = ?"
     core/user-id]

    :active-users-by-joined-at
    ["select _id from users where user$joined_at > ?"
     (Timestamp. (.getTime #inst "2025"))]

    :active-users-by-viewed-at
    ["select distinct user_item$user from user_items where user_item$viewed_at > ?"
     (Timestamp. (.getTime #inst "2025"))]

    :active-users-by-ad-updated
    ["select ad$user from ads where ad$updated_at > ?"
     (Timestamp. (.getTime #inst "2025"))]

    :active-users-by-ad-clicked
    ["select distinct ad$click$user from ad_clicks where ad$click$created_at > ?"
     (Timestamp. (.getTime #inst "2025"))]

    :feeds-to-sync
    (let [{user-ids :uuids} (core/read-fixture "active-user-ids.edn")]
      (vec
       (concat [(str "select distinct sub$feed$feed
                      from subs
                      join feeds on feeds._id = sub$feed$feed
                      where sub$user in " (core/?s (count user-ids))
                     " and (feed$synced_at is null or feed$synced_at < ?)")]
               user-ids
               [(Timestamp. (- (.getTime core/latest-t)
                               (* 1000 60 60 2)))])))

    :existing-feed-titles
    (let [{:keys [id-uuid real-titles random-titles]} (core/read-fixture "biggest-feed.edn")
          titles (concat real-titles random-titles)]
      (vec
       (concat [(str "select item$title
                      from items
                      where item$feed$feed = ?
                      and item$title in " (core/?s (count titles)))
                id-uuid]
               titles)))}})

(defonce node nil)

(defn q [& query]
  (with-open [conn (get-conn node)]
    (xt/q conn (vec query))))

(defn q-benchmark [id]
  (apply q (get-in benchmarks [:queries id])))

(comment
  (def node (start-node))
  (def conn (get-conn node))
  (.close conn)
  (.close node)

  (count (time (q-benchmark :existing-feed-titles)))
  (inc 3)

  )
