(ns dataset-analyzer.core-test
  (:require [clojure.test :refer :all]
            [dataset-analyzer.core :refer :all]
            [incanter.core :refer :all]))

(def ds (dataset [:a :b :c :d :e] [["2014-07-13" 1 23.4 "aa-13" 12]
                                    ["2014-02-12" 2 18.2 nil 32.1]
                                    ["2014-11-21" 3 nil "abcd-043" "ba"]]))
(def a ($ :a ds))
(def b ($ :b ds))
(def c ($ :c ds))
(def d ($ :d ds))
(def e ($ :e ds))

(deftest test-col-types
  (is (= (check-col-type a) :string))
  (is (= (check-col-type b) :int))
  (is (= (check-col-type c) :float))
  (is (= (check-col-type c :allow-nil false) :mixed))
  (is (= (check-col-type d) :string))
  (is (= (check-col-type d :allow-nil false) :mixed))
  (is (= (check-col-type e) :mixed))
  (is (= (col-types ds) [:string :int :float :string :mixed]))
  (is (= (col-types ds :allow-nil false) [:string :int :mixed :mixed :mixed]))
  )

(check-col-type d)

(deftest test-col-ops
  (is (= (filter-on-col-type ds :string) ($ [:a :d] ds)))
  (is (= (col-interval a) [10, 10]))
  (is (= (col-interval b) [1, 3]))
  (is (= (col-interval c) [18.2, 23.4]))
  (is (= (col-interval d) [5, 8]))
  (is (= (col-interval e) nil))
  )

(deftest test-histogram
  (is (= (histogram [1 3 2 2 5] :bins 5)
         [[1 2 1 0 1] [1 9/5 13/5 17/5 21/5 5]]))
  (is (= (histogram [1 2 1] :bins 3)
         [[2 0 1] [1 4/3 5/3 2]])))


(deftest test-create-regex
  (let [testfn (fn [regex1 regex2]
                 (is (= (str regex1) (str regex2))))]
    (testfn (create-regex a :include-interval true)
             #"\d{4}\-\d{2}\-\d{2}")
    (testfn (create-regex a :include-interval false)
             #"\d+\-\d+\-\d+")
    (testfn (create-regex d :include-interval true)
            #"[a-zA-Z]{2,4}\-\d{2,3}")
    (testfn (create-regex d :include-interval false)
            #"[a-zA-Z]+\-\d+")))


(run-tests)
