(ns hammer.core
  (:require   [clojure.java.io :as io]
              [clojure.data.csv :as csv]
              [pjstadig.reducible-stream :refer [decode-lines!]]))

(defn line-csv-data->maps
  [titles line]
  (zipmap
   titles
   (-> line
       (clojure.string/replace #"\"" "")
       (clojure.string/split #","))))

(defn csv-titles
  [filename]
  (with-open [reader (io/reader filename)]
    (let [first-line (first (line-seq reader))]
      (->> (clojure.string/split first-line #",")
           (map clojure.string/lower-case)
           (map #(clojure.string/replace % #"[\(\)]" ""))
           (map #(clojure.string/replace % #"\"" ""))
           (map #(clojure.string/replace % #"( +)" "-"))
           (map keyword) ;; Drop if you want string keys instead
           ))))

(defn lazy-csv-to-maps
  [file-path]
  (let [titles (csv-titles file-path)
        source (decode-lines! (io/input-stream file-path))
        transducer (map (partial line-csv-data->maps titles))]
    (into [] transducer (next source))))

(defn replace-empty-string-by-nil
  "in csv, an empty string \"\" often means no value, so in clojure that's nil"
  [x]
  (if (= x "")
    nil
    x))

(defn nil-percentage
  "counts the nils and calculates a percentage.  could be generalized?"
  ([] [0 0])
  ([[nil-count n] e]
   (if (nil? e)
     [(inc nil-count) (inc n)]
     [nil-count (inc n)]))
  ([[nil-count n]]
   (float (/ nil-count n))))

(defn- join-indexed [vec1 indexed-vec2 keys]
  (vec (flatten (map (fn [row1]  (let [rows-to-join (get indexed-vec2 (select-keys row1 keys))]
                                   (if rows-to-join
                                     (map #(merge row1 %1) rows-to-join)
                                     row1))) vec1))))

(defn- join-indexed-mapped [vec1 indexed-vec2 join-keys]
  (vec (flatten (map (fn [row1]  (let [index-key (clojure.set/rename-keys (select-keys row1 (keys join-keys)) join-keys)
                                       rows-to-join (get indexed-vec2 index-key)]
                                   (if rows-to-join
                                     (map #(merge row1 %1) rows-to-join)
                                     row1))) vec1))))

(defn- construct-index [data keys]
  (let [compound-key (fn [row] (select-keys row keys))
        row-without-join (fn [row] (apply dissoc row keys))
        all-indexed-data (map (fn [row] [(compound-key row) (row-without-join row)]) data)
        grouped-data (group-by (fn [[key row]] key) all-indexed-data)]
    (reduce-kv (fn [m key value] (assoc m key (vec (map (fn [[k2 v2]] v2) value))) ) {} grouped-data)))

(defn left-outer-join
  "takes two vectors of maps and returns the left outer join on specified keys - keys is an array of keys or a map of corresponding keys"
  [vec1 vec2 join-keys]
  (if (map? join-keys)
    (let [indexed-vec2 (construct-index vec2 (vals join-keys))]
      (join-indexed-mapped vec1 indexed-vec2 join-keys))
    (let [indexed-vec2 (construct-index vec2 join-keys)]
      (join-indexed vec1 indexed-vec2 join-keys))))

;; copied from Medley https://github.com/weavejester/medley
(defn- editable? [coll]
  (instance? clojure.lang.IEditableCollection coll))

(defn- reduce-map [f coll]
  (if (editable? coll)
    (persistent! (reduce-kv (f assoc!) (transient (empty coll)) coll))
    (reduce-kv (f assoc) (empty coll) coll)))

(defn filter-vals
  "Returns a new associative collection of the items in coll for which
  `(pred (val item))` returns true."
  [pred coll]
  (reduce-map (fn [xf] (fn [m k v] (if (pred v) (xf m k v) m))) coll))
