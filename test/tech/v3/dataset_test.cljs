(ns tech.v3.dataset-test
  (:require [cljs.test :refer [deftest is run-tests]]
            [tech.v3.dataset :as ds]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.datetime :as dtype-dt]))


(deftest base-mapseq-test
  (let [ds (ds/->dataset [{:a 1 :b 2} {:a 1} {:b 2}])]
    (is (= #{1 2} (ds/missing ds))))

  (let [ds (ds/->dataset [{:a "hey" :b :hey :c true}
                          {:a "you" :b :you :c false}])]
    (is (= #{:string :keyword :boolean}
           (->> (vals ds)
                (map (comp :datatype meta))
                (set))))))


(deftest base-colmap-test
  (let [ds (ds/->dataset {:a [1 2 nil 3]
                          :b [1.01 2.02 3.03 4.04]})]
    (is (= #{:float64} (->> (vals ds) (map (comp :datatype meta)) (set))))
    (is (= #{2} (ds/missing ds))))
  ;;test the one fastpath
  (let [ds (ds/->dataset {:a (dtype/make-container :float32 (concat
                                                             (range 100)
                                                             [##NaN ##NaN]
                                                             (range 100)))})]
    (is (= #{100 101} (ds/missing ds)))
    (is (= 202 (ds/row-count ds)))))


(deftest colum-map-test
  (let [ds (-> (ds/->dataset {:a (range 10)})
               (ds/column-map :b (partial + 2)))]
    (is (= 45 (apply + (ds :a))))
    (is (= 65 (apply + (ds :b))))))


(defn master-ds
  []
  (ds/->dataset {:a (range 5)
                 :b (repeat 5 :a)
                 :c (repeat 5 "hey")
                 :d (repeat 5 {:a 1 :b 2})
                 :e (repeat 4 [1 2 3])
                 :f (repeat 5 (dtype-dt/local-date))
                 :g (repeat 5 (dtype-dt/instant))}))


(deftest serialize-test
  (let [ds (master-ds)
        json-data (ds/dataset->transit-str ds)
        nds (ds/transit-str->dataset json-data)]
    (is (= (vec (range 5))
           (vec (nds :a))))
    (is (= #{4} (ds/missing nds)))
    (is (= (mapv (comp :datatype meta) (vals ds))
           (mapv (comp :datatype meta) (vals nds))))))


;;We have to remove the instants as they contain nanosecond data
(defn master-ds-equiv [] (dissoc (master-ds) :g))


(deftest hashcode-equiv
  (is (= (master-ds-equiv) (master-ds-equiv)))
  (is (not= (master-ds-equiv) (ds/rename-columns (master-ds-equiv) {:a :aa})))
  (is (= (hash (master-ds-equiv)) (hash (master-ds-equiv))))
  (is (not= (hash (master-ds-equiv)) (hash (ds/rename-columns (master-ds-equiv) {:a :aa})))))


(deftest replace-missing
  (let [ds (ds/->dataset {:a [nil 2 nil nil nil 6 nil]})]
    (is (= [2 2 2 2 2 6 6] ((ds/replace-missing ds :all :first) :a)))
    (is (= [2 2 6 6 6 6 6] ((ds/replace-missing ds :all :last) :a)))
    (is (= [2 2 3 4 5 6 6] ((ds/replace-missing ds :all :lerp) :a)))
    (is (= [20 2 20 20 20 6 20] ((ds/replace-missing ds :all [:value 20]) :a)))))


(deftest replace-missing-str
  (let [ds (ds/->dataset {:a [nil "hey" nil nil nil "you" nil]})]
    (is (= ["hey" "hey" "hey" "hey" "hey" "you" "you"]
           ((ds/replace-missing ds :all :first) :a)))
    (is (= ["hey" "hey" "you" "you" "you" "you" "you"]
           ((ds/replace-missing ds :all :last) :a)))
    (is (= ["guys" "hey" "guys" "guys" "guys" "you" "guys"]
           ((ds/replace-missing ds :all [:value "guys"]) :a)))))


(deftest concat-missing
  (let [ds (ds/->dataset {:a [nil 2 nil nil nil 6 nil]})
        ds (ds/concat ds ds)]
    (is (= [20 2 20 20 20 6 20 20 2 20 20 20 6 20]
           ((ds/replace-missing ds :all [:value 20]) :a)))))


(deftest concat-missing-columns
  (let [ds (ds/concat
            (ds/->dataset {:a (range 10)
                           :c (repeat 10 (dtype-dt/local-date))})
            (ds/->dataset {:b (range 10)}))]
    (is (= 20 (ds/row-count ds)))
    (is (= 10 (dtype/ecount (ds/missing (ds :a)))))
    (is (= 10 (dtype/ecount (ds/missing (ds :b)))))))


(deftest row-hash-equiv
  (let [ds (ds/->dataset {:a (range 100)
                          :b (range 100)
                          :c (repeat 100 :b)})]
    (is (= {:a 0 :b 0 :c :b}
           (ds/row-at ds 0)))
    (is (= (hash {:a 0 :b 0 :c :b})
           (hash (ds/row-at ds 0))))
    (is (= [0 0 :b]
           (ds/rowvec-at ds 0)))
    (is (= (hash [0 0 :b])
           (hash (ds/rowvec-at ds 0))))))


(deftest stocks-test
  (let [stocks (ds/transit-file->dataset "test/data/stocks.transit-json")
        stock-agg (->> (ds/group-by-column stocks :symbol)
                       (map (fn [[k v]]
                              [k (-> (dfn/mean (v :price))
                                     (* 100)
                                     (Math/round)
                                     (/ 100.0))]))
                       (into {}))]
    ;;these are taken directly from the jvm aggregation so the numbers have to be identical.
    (is (= {"MSFT" 24.74, "AMZN" 47.99, "IBM" 91.26, "GOOG" 415.87, "AAPL" 64.73}
           stock-agg))))
