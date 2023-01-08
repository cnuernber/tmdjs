(ns tech.v3.dataset-test
  (:require [cljs.test :refer [deftest is run-tests]]
            [tech.v3.dataset :as ds]
            [tech.v3.dataset.node :as ds-node]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.argops :as argops]
            [tech.v3.datatype.datetime :as dtype-dt]
            [ham-fisted.api :as hamf]))


(deftest index-reducer-tests
  (is (= [1 1 1] (vec (hamf/reduce-reducer argops/index-reducer-rf [1 1 1]))))
  (is (= [-1 -2 -3] (vec (hamf/reduce-reducer argops/index-reducer-rf [-1 -2 -3]))))
  (is (= [1 2 3] (vec (hamf/reduce-reducer argops/index-reducer-rf [1 2 3]))))
  (is (= [1 2] (vec (hamf/reduce-reducer argops/index-reducer-rf [1 2]))))
  (is (= [1] (vec (hamf/reduce-reducer argops/index-reducer-rf [1]))))
  (is (= [] (vec (hamf/reduce-reducer argops/index-reducer-rf [])))))


(deftest nth-neg-indexes
  (let [data (dtype/make-container :int32 (range 10))]
    (is (thrown? js/Error (nth data 10)))
    (is (= :a (nth data 10 :a)))
    (is (thrown? js/Error (nth data -11)))
    (is (= :a (nth data -11 :a)))
    (is (= 0 (nth data -10 :a))))
  (let [data (dtype/make-list :int32 10)]
    (dotimes [idx 10] (dtype/add! data idx))
    (is (thrown? js/Error (nth data 10)))
    (is (= :a (nth data 10 :a)))
    (is (thrown? js/Error (nth data -11)))
    (is (= :a (nth data -11 :a)))
    (is (= 0 (nth data -10 :a))))
  (let [data ((ds/->dataset {:a (range 10)}) :a)]
    (is (thrown? js/Error (nth data 10)))
    (is (= :a (nth data 10 :a)))
    (is (thrown? js/Error (nth data -11)))
    (is (= :a (nth data -11 :a)))
    (is (= 0 (nth data -10 :a)))))


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


(deftest column-map-test
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
  (let [stocks (ds-node/transit-file->dataset "test/data/stocks.transit-json")
        stock-agg (->> (ds/group-by-column stocks :symbol)
                       (map (fn [[k v]]
                              [k (-> (dfn/mean (v :price))
                                     (* 100)
                                     (Math/round)
                                     (/ 100.0))]))
                       (into {}))]
    ;;these are taken directly from the jvm aggregation so the numbers have to be identical.
    (is (= {"MSFT" 24.74, "AMZN" 47.99, "IBM" 91.26, "GOOG" 415.87, "AAPL" 64.73}
           stock-agg))
    (let [stock-ds (ds/->dataset {:symbol (keys stock-agg)
                                  :means (vals stock-agg)})
          stock-groups (ds/group-by-column stocks :symbol)
          ;;this was failing due to equiv being broken for datasets when not comparing
          ;;to other datasets
          final-ds (ds/column-map stock-ds :src-data stock-groups [:symbol])]
      (is (= 5 (ds/row-count final-ds))))))


(deftest row-map-edge-case
  (let [ds (-> (ds/->dataset {:a (range 10)})
               (ds/row-map (fn [row]
                             (when (> (row :a) 4)
                               {:b (+ (row :a) 2)}))))]
    (is (every? dfn/scalar-eq
                (map vector
                     [##NaN ##NaN ##NaN ##NaN ##NaN 7 8 9 10 11]
                     (vec (ds :b)))))))


(deftest vec-sub-buffer
  (is (= [0 1 2 3]
         (dtype/sub-buffer (vec (range 10)) 0 4))))


(deftest remove-rows-missing
  (let [ds (ds/->dataset {:a [nil 2 nil nil nil 6 nil 8 7 6]
                          :b [2 nil nil nil 3 4 5 nil 8 9]})
        ds (ds/remove-rows ds (dtype/set-and (ds/missing (ds :a))
                                             (ds/missing (ds :b))))]
    (is (= 8 (ds/row-count ds)))))


(defn nan-eq
  [lhs rhs]
  (->> (map vector lhs rhs)
       (every? #(dfn/scalar-eq (% 0) (% 1)))))


(deftest sort-works-with-nan
  (let [ds (ds/->dataset {:a [1 nil 2 nil nil 4]} )
        ds-first (ds/sort-by-column ds :a nil {:nan-strategy :first})
        ds-last (ds/sort-by-column ds :a nil {:nan-strategy :last})]
    (is (nan-eq [##NaN ##NaN ##NaN 1 2 4] (ds-first :a)))
    (is (nan-eq [1 2 4 ##NaN ##NaN ##NaN] (ds-last :a)))
    (is (thrown? js/Error (ds/sort-by-column ds :a nil {:nan-strategy :exception})))))


(deftest sort-allows-custom-comparator
  (let [ds (ds/->dataset {:a [1 nil 2 nil nil 4]})
        asc (ds/sort-by-column ds :a nil {:comparator #(compare %1 %2)})
        desc (ds/sort-by-column ds :a nil {:comparator #(compare %2 %1)})]
    (is (nan-eq [1 2 4 ##NaN ##NaN ##NaN] (asc :a)))
    (is (nan-eq [4 2 1 ##NaN ##NaN ##NaN] (desc :a)))))


(deftest completely-filtered-tables-fail-print
  (let [ds (-> (ds/->dataset {:a (range 10)})
               (ds/filter-column :a #(> % 10)))]
    (is (not (nil? (pr-str ds))))))


(deftest ds-concat-nil-seq
  (is (nil? (apply ds/concat nil))))


(deftest sorting-objects
  (let [ds (ds/->dataset {:a [nil nil 1.0 2.0]
                          :b ["hey" "you" nil nil]})]
    (is (= ["hey" "you" nil nil]
           (->> (ds/sort-by-column ds :b nil {:nan-strategy :last})
                :b
                (vec))))
    (is (= [nil nil "hey" "you"]
           (->> (ds/sort-by-column ds :b nil {:nan-strategy :first})
                :b
                (vec))))
    (is (nan-eq [1 2 ##NaN ##NaN]
                (->> (ds/sort-by-column ds :a nil {:nan-strategy :last})
                     :a
                     (vec))))
    (is (nan-eq [##NaN ##NaN 1 2]
                (->> (ds/sort-by-column ds :a nil {:nan-strategy :first})
                     :a
                     (vec))))))


(deftest double-nan-missing
  (let [ds (ds/->dataset {:a [0.0 js/NaN 1.0]
                          :b [0 js/NaN 1.0]
                          :c [:a nil :b]})]
    (is (= #{1} (set (ds/missing (ds :a)))))
    (is (= #{1} (set (ds/missing (ds :b)))))
    (is (= #{1} (set (ds/missing (ds :c)))))
    (is (= [1.0] (-> (ds/filter-column ds :a)
                     (ds/column :a)
                     (vec))))
    (is (= [1.0] (-> (ds/filter-column ds :b)
                   (ds/column :a)
                   (vec))))
    (is (= [0 1] (-> (ds/filter-column ds :c)
                     (ds/column :a)
                     (vec))))))


(deftest boolean-containers
  (let [data (dtype/make-container :boolean [true false true])]
    (is (= false (data 1)))))


(deftest text-test
  (let [text-ds (ds-node/transit-file->dataset "test/data/text.transit-json")]
    ;;text data gets changed into string data on the client side as the cljs dataset
    ;;doesn't have a text datatype.
    (is (= ["text" "text" "text" "text" "text"] (text-ds :i)))))
