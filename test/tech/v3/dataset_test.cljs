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
        json-data (ds/dataset->json ds)
        nds (ds/json->dataset json-data)]
    (is (= (vec (range 5))
           (vec (nds :a))))
    (is (= #{4} (ds/missing nds)))
    (is (= (mapv (comp :datatype meta) (vals ds))
           (mapv (comp :datatype meta) (vals nds))))))
