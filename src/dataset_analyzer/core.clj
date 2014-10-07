(ns dataset-analyzer.core)

(use '(incanter core io datasets stats))
(use 'clojure.pprint)
(use '[clojure.string :only (join)])

(require '[clojure.data.csv :as csv]
         '[clojure.java.io :as io]
         '[clojure.data.json :as json])

;(def ds (read-dataset "/Volumes/untitled/TableRating_small.txt" :header true))

(defn col-apply [column fun & {:keys [remove-nil]
                               :or {remove-nil true}}]
  "Apply fun to column with name col-name"
  (if (true? remove-nil)
    (fun (filter (complement nil?) column))
    (fun column)))

(defn col-mean [column]
  (col-apply column mean))

(defn col-max [column]
  (apply max (filter (complement nil?) column)))

(defn col-min [column]
  (apply min (filter (complement nil?) column)))

(defn col-std [column]
  (col-apply column sd))

(defn col-var [column]
  (col-apply column variance))

(def allowed-ops-on-type {:float [:mean :max :min :std :var :interval]
                          :int [:mean :max :min :std :var :interval :nr-unique]
                          :string [:interval :nr-unique :create-regex]
                          :mixed []})

(defn value-type [val]
  "Identify type of given value; string, int, float, bool or empty/nil"
  (cond
   (float? val) :float
   (number? val) :int
   (string? val) :string
   (nil? val) nil
   :else :mixed))

(defn check-col-type [column & {:keys [allow-nil]
                          :or {allow-nil true}}]
  "Determine column type of column; string, int, float or nil (mixed)
   If allow-nil is true we allow nil values in a column, if false all columns
   with any nil values give column type nil"
  (let [type-check (fn [t t2]
                     (cond
                       (= t t2) t
                       (and (or (= t :float) (= t :int))
                            (or (= t2 :float) (= t2 :int))) :float
                       :else (if (and (nil? t2) allow-nil)
                               t
                               :mixed)
                       ))]
    (loop [column column
           t (value-type (first column))]
      (if (not (seq column))
        t
        (recur (rest column)
               (type-check t (value-type (first column))))
      ))))

(defn col-types [ds & {:keys [allow-nil]
                       :or {allow-nil true}}]
  "Get column types as list"
  (map #(check-col-type ($ % ds) :allow-nil allow-nil)
       (col-names ds)))


(defn filter-on-col-type [ds col-type]
  "Return dataset with only columns of given type"
  (let [cols (map vector (col-names ds) (col-types ds))
        col-names (map first (filter #(= (second %) col-type) cols))]
    (sel ds :cols col-names))
  )

(defn col-interval [column]
  "Get length intervals of values in column.
  If string-type it returns min, max length of column.
  If int/float-type it returns min, max value in column
  "
  (let [col-type (check-col-type column)
        column (filter (complement nil?) column)]
    (cond
     (= col-type :string) [(count (apply min-key count column))
                           (count (apply max-key count column))]
     (or (= col-type :int) (= col-type :float)) [(col-min column)
                                                 (col-max column)]
     :else nil
     )
  ))

(defn histogram [column & {:keys [bins minv maxv]
                            :or {bins 10 minv nil maxv nil}}]
  "Return array of length nr-bins with columns values
  TODO: bins as sequence "
  (let [minv (if (not (nil? minv))
               minv
               (col-min column))
        maxv (if (not (nil? maxv))
               maxv
               (col-max column))
        step (/ (- maxv minv) bins)
        bin-edges (conj (vec (range minv maxv step)) maxv)
        bucket-size (/ (- maxv minv) bins)
        get-bin (fn [v]
                  (int (- (Math/ceil (/ (- v minv) bucket-size)) 1)))]
    (loop [hist (vec (repeat bins 0))
           column column]
      (if (not (seq column))
        [hist bin-edges]
        (let [bin-nr (max 0 (min bins (get-bin (first column))))]

            (recur (assoc hist bin-nr (inc (nth hist bin-nr)))
                   (rest column)))))))

(defn escape-all [x]
    (str "\\" (reduce #(str  %1 "\\" %2) x)))

(defn to-regex-vector [ss]
  (let [char-to-regex (fn [c] (cond
                               (re-matches #"\d" c) #"\d+"
                               (re-matches #"[a-zA-Z]" c) #"[a-zA-Z]+"
                               (re-matches #"\s" c) #"\s+"
                               :else (re-pattern (escape-all c))))]
    (loop [form []
           ss ss]
      (if (not (seq ss))
        form
        (let [first-char (char-to-regex (str (first ss)))
              bite (re-find first-char ss)
              bite-len (count bite)]
          (if (= bite-len 0)
            form
            (recur (conj form [first-char bite-len bite-len])
                   (subs ss bite-len))))))))

(defn regex-vec-combiner [v1 v2]
  (loop [v1 v1
         v2 v2
         regex []]
    (if (not (seq v1))
      regex
      (let [r1 (first v1)
            r2 (first v2)
            r1-min (nth r1 1)
            r2-min (nth r2 1)
            r1-max (nth r1 2)
            r2-max (nth r2 2)]
        (if (= (str (nth r1 0)) (str (nth r2 0)))
          (recur (rest v1) (rest v2)
                 (conj regex [(nth r1 0)
                              (min r1-min r2-min)
                              (max r1-max r2-max)])))))))

(defn create-regex [column & {:keys [include-interval]
                             :or {include-interval true}}]
  "Create regex that describes the columns values.
  Only works on columns with type :string"
  (let [column (filter (complement nil?) column)
        regex-vector (reduce regex-vec-combiner
                       (map to-regex-vector column))
        regex-vec-to-string (fn [v]
                              (let [regex (str (v 0))
                                    regex (if (= \+ (last regex))
                                            (apply str (butlast regex))
                                            regex)
                                    start (v 1)
                                    end (v 2)
                                    interval (if (= start end)
                                               (if (= 1 start)
                                                 ""
                                                 (if include-interval
                                                   (format "{%s}" start)
                                                   "+"))
                                               (if include-interval
                                                 (format "{%s,%s}" start end)
                                                 "+"))]

                              (format "%s%s" regex interval)))]
    (re-pattern (join (map regex-vec-to-string regex-vector))
     )))

(def kw-to-col-op
  {:mean col-mean, :min col-min, :max col-max, :std col-std, :var col-var,
   :type check-col-type :nr-unique #(count (distinct %))
   :interval col-interval :create-regex create-regex})

(defn get-stats [ds]
  "Get basic stats from dataset."
  (loop [col-names (col-names ds)
         stats []]
    (let [col-name (first col-names)
          column ($ col-name ds)
          col-type (check-col-type column)
          col-ops (allowed-ops-on-type col-type)
          col-stats (merge {:name col-name :type col-type}
                      (apply merge
                             (map
                              #(hash-map % ((kw-to-col-op %) column))
                              col-ops)))
          ]
      (if (not (seq col-names))
        stats
        (recur (rest col-names) (conj stats col-stats))))
  ))


(defn stats-to-csv [stats out-path]
  "Save stats to CSV-file."
  (let [header (list* :name :type (disj (set (apply concat (map keys stats)))
                                   :name :type))
        get-row (fn [stat]
                 (loop [hs header
                        row []]
                  (let [h (first hs)
                        v (if (contains? stat h)
                            (get stat h)
                            nil)]
                    (if (not (seq hs))
                      row
                      (recur (rest hs) (conj row v))))))]
    (with-open [out-file (io/writer out-path)]
      (do
        (csv/write-csv out-file (vector header))
        (csv/write-csv out-file (map get-row stats))
      )
  )))


(defn stats-to-json [stats]
  json/write-str stats)




