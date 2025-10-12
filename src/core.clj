(ns core
  (:require
   [clojure.java.io :as io]
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

(defn test-benchmarks [conn benchmarks]
  (doseq [{:keys [id f expected]} benchmarks
          :let [actual (f conn)]]
    (assert (= expected actual)
            (str "Benchmark " id " results are incorrect. Actual: "
                 (pr-str actual) ", expected: " (pr-str expected)))))

(defn run-benchmarks [conn benchmarks]
  (let [[_ pstats] (tufte/profiled
                    {}
                    (doseq [{:keys [id f n] :or {n 10}} benchmarks
                            _ (range n)]
                      (p id (f conn))))]
    (println (tufte/format-pstats @pstats))))

(defn check-benchmark [conn benchmarks id]
  ((->> benchmarks
        (filterv #(= (:id %) id))
        first
        :f)
   conn))
