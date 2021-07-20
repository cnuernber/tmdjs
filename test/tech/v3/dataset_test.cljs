(ns tech.v3.dataset-test
  (:require [cljs.test :refer [deftest is run-tests]]
            [tech.v3.dataset :as ds]
            [tech.v3.datatype :as dtype]))


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
                 :e (repeat 4 [1 2 3])}))


(deftest serialize-test
  (let [ds (master-ds)
        json-data (ds/dataset->transit-str ds)
        nds (ds/transit-str->dataset json-data)]
    (is (= (vec (range 5))
           (vec (nds :a))))
    (is (= #{4} (ds/missing nds)))
    (is (= (mapv (comp :datatype meta) (vals ds))
           (mapv (comp :datatype meta) (vals nds))))))


(deftest hashcode-equiv
  (is (= (master-ds) (master-ds)))
  (is (not= (master-ds) (ds/rename-columns (master-ds) {:a :aa})))
  (is (= (hash (master-ds)) (hash (master-ds))))
  (is (not= (hash (master-ds)) (hash (ds/rename-columns (master-ds) {:a :aa})))))


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
