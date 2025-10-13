(ns xtdb2
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.walk :as walk]
   [core]
   [taoensso.nippy :as nippy]
   [xtdb.api :as xt]
   [xtdb.node :as xtn]))

(def expected-user
  {:user/email-username "Sn0a6"
   :user/digest-last-sent #xt/zdt "2025-09-28T18:07:26.604435Z[UTC]"
   :user/send-digest-at #xt/time "08:00"
   :user/roles #{:admin}
   :user/customer-id "JIKApV1OsDDIp9uKRb"
   :user/email "w6qhyZcYmAcXOoLWrq"
   :user/digest-days #{:saturday :tuesday :wednesday :sunday :friday :monday :thursday}
   :xt/id #uuid "e86e5e14-0001-46eb-9d11-134162ce930f"
   :user/use-original-links false})

(def benchmarks
  [{:id       :get-user-by-email
    :expected [expected-user]
    :f        #(xt/q % ["select * from users where user$email = ?" core/user-email])}
   {:id       :get-user-by-id
    :expected [expected-user]
    :f        #(xt/q % ["select * from users where _id = ?" core/user-id])}
   {:id       :get-user-id-by-email
    :expected [{:xt/id core/user-id}]
    :f        #(xt/q % ["select _id from users where user$email = ?" core/user-email])}
   {:id       :get-user-email-by-id
    :expected [{:user/email core/user-email}]
    :f        #(xt/q % ["select user$email from users where _id = ?"
                          core/user-id])}
   {:id       :get-feeds
    :expected [{:xt/column-1 162}]
    :f        #(xt/q % [(str "select count(sub$feed$feed) from subs "
                             "where sub$user = ? and sub$feed$feed is not null")
                          core/user-id])}
   {:id       :get-items
    :expected [{:xt/column-1 11284}]
    :f        #(xt/q % [(str "select count(i._id) "
                             "from subs s "
                             "join items i on i.item$feed$feed = s.sub$feed$feed "
                             "where s.sub$user = ? "
                             "and s.sub$feed$feed is not null")
                        core/user-id])}])

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

(defn benchmark []
  (println "benchmarking XTDB 2")
  (with-open [node (start-node)
              conn (get-conn node)]
    (core/test-benchmarks conn benchmarks) ; warm up
    (core/run-benchmarks conn benchmarks)))

(defn -main [command]
  (case command
    "setup" (setup)
    "benchmark" (benchmark))
  (System/exit 0))
