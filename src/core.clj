(ns core
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.pprint :refer [pprint print-table]]
   [clojure.string :as str]
   [clojure.walk :as walk]
   [taoensso.nippy :as nippy]
   [taoensso.tufte :as tufte :refer [p]]))

(def input-info
  [{:attr :user/email, :dir "users"}
   {:attr :feed/url, :dir "feeds"}
   {:attr :sub/user, :dir "subs"}
   {:attr :item/ingested-at, :dir "items"}
   {:attr :user-item/user, :dir "user-items"}
   {:attr :ad/user, :dir "ads"}
   {:attr :ad.click/user, :dir "ad-clicks"}
   {:attr :ad.credit/ad, :dir "ad-credits"}
   {:attr :digest/user, :dir "digests"}
   {:attr :bulk-send/sent-at, :dir "bulk-sends"}
   {:attr :skip/user, :dir "skips"}])

(defn read-docs [dir]
  (->> (file-seq (io/file "import" dir))
       (filterv #(.isFile %))
       sort
       (mapcat nippy/thaw-from-file)))

(defn random-string [length]
  (let [chars (concat (map char (range 48 58))   ;; 0-9
                      (map char (range 65 91))   ;; A-Z
                      (map char (range 97 123))) ;; a-z
        sb (StringBuilder.)]
    (dotimes [_ length]
      (.append sb (rand-nth chars)))
    (.toString sb)))

(defn anonymize []
  (doseq [{:keys [dir]} input-info
          input-file (file-seq (io/file "import" dir))
          :when (.isFile input-file)
          :let [output-file (io/file (str "anonymized/" dir) (.getName input-file))]]
    (io/make-parents output-file)
    (->> (nippy/thaw-from-file input-file)
         (mapv (fn [doc]
                 (walk/postwalk
                  (fn [x]
                    (cond
                      (string? x)
                      (random-string (count x))

                      (int? x)
                      (rand-int 1000)

                      :else
                      x))
                  (dissoc doc :user/timezone))))
         (nippy/freeze-to-file output-file))))

(def user-id #uuid "e86e5e14-0001-46eb-9d11-134162ce930f")
(def user-email "w6qhyZcYmAcXOoLWrq")
(def user-id-int 4399)
(def latest-t #inst "2025-09-29T05:36:23Z")

(defn read-fixture [f]
  ;; for some reason io/resource is giving me nil when used over remote nrepl (???)
  (edn/read-string (slurp (io/file "resources" f))))

(defn ?s [n]
  (str "(" (str/join ", " (repeat n "?")) ")"))

(defn utc-date [instant]
    (let [zone-id (java.time.ZoneId/of "UTC")
          local-date (.toLocalDate (.atZone instant zone-id))]
      (str local-date)))

(defn benchmarks-for [db-name]
  @(requiring-resolve (symbol db-name "benchmarks")))

(defn setup [db-name]
  ((:setup (benchmarks-for db-name))))

(defmacro catch-timeout [& body]
  `(try
     ~@body
     (catch java.util.concurrent.TimeoutException e#
       :timeout)))

(defn update-expected [db-name]
  (let [{:keys [with-conn run-query queries migrate]} (benchmarks-for db-name)]
    (when migrate
      (migrate))
    (with-conn
      (fn [conn]
        (let [results (vec
                       (for [[id query] queries]
                         {:id id :expected (catch-timeout (count (run-query conn query)))}))]
          (io/make-parents "expected/_")
          (spit (str "expected/" db-name ".edn")
                (str ";; auto-generated; do not edit.\n" (with-out-str (pprint results))))
          (nippy/freeze-to-file (str "expected/" db-name ".nippy") results))))))

(defn benchmark [db-name]
  (let [{:keys [with-conn run-query queries migrate]} (benchmarks-for db-name)]
    (when migrate
      (migrate))
    (with-conn
      (fn [conn]
        ;; test (and warm up caches)
        (let [id->expected (->> (nippy/thaw-from-file (str "expected/" db-name ".nippy"))
                                (map (juxt :id :expected))
                                (into {}))]
          (doseq [[id query] queries
                  :let [expected (get id->expected id)
                        actual (catch-timeout (count (run-query conn query)))]
                  :when (not (some #{:timeout} [actual expected]))]
            (assert (= expected actual)
                    (str "Benchmark " id " results are incorrect. Actual: "
                         (pr-str actual) ", expected: " (pr-str expected)))))

        ;; run
        (let [[_ pstats] (tufte/profiled
                          {}
                          (doseq [[id query] queries
                                  _ (range 10)]
                            (p id (catch-timeout (run-query conn query)))))]
          (io/make-parents "results/_")
          (spit (str "results/" db-name ".edn")
                (with-out-str (pprint @pstats)))
          (println (tufte/format-pstats @pstats)))))))

(defn report []
  (let [results (->> (file-seq (io/file "results"))
                     (filterv #(.isFile %))
                     (mapcat (fn [f]
                               (let [db-name (-> (.getName f)
                                                 (str/split #"\.")
                                                 first)]
                                 (for [[id result] (:stats (edn/read-string (slurp f)))]
                                   (assoc result :id id :db-name db-name)))))
                     (group-by :id))
        results (-> results
                    (update-vals
                     (fn [results]
                       (let [postgres-p50 (some (fn [{:keys [db-name p50]}]
                                                  (when (= db-name "postgres")
                                                    p50))
                                                results)]
                         (into {"query" (:id (first results))
                                "xtdb2" nil
                                "xtdb1" nil
                                "postgres" nil
                                "sqlite" nil}
                               (map (fn [{:keys [db-name p50]}]
                                      (let [p50-ms (some-> p50 (/ (* 1000 1000.0)))
                                            p50-factor (when (and p50 postgres-p50)
                                                         (/ p50 postgres-p50 1.0))]
                                        [db-name {:p50-ms p50-ms
                                                  :p50-factor p50-factor}])))
                               results))))
                    vals)]
    (println "p50 run times:")
    (print-table ["query"
                  "xtdb2 (ms)"
                  "xtdb2 (factor)"
                  "xtdb1 (ms)"
                  "xtdb1 (factor)"
                  "sqlite (ms)"
                  "sqlite (factor)"
                  "postgres (ms)"]
                 (->> results
                      (sort-by #(get-in % ["xtdb2" :p50-ms]) #(compare %2 %1))
                      (mapv (fn [row]
                              (into {}
                                    (mapcat (fn [[k v]]
                                              (if (= k "query")
                                                [[k v]]
                                                [[(str k " (ms)") (some->> (:p50-ms v) (format "%.03f"))]
                                                 [(str k " (factor)")
                                                  (some->> (:p50-factor v) (format "%.01f"))] ])))
                                    row)))))))

(defn -main [command & args]
  (apply println "Running" command args)
  (time
   (apply (case command
            "setup" setup
            "update-expected" update-expected
            "benchmark" benchmark
            "report" report)
          args))
  (System/exit 0))
