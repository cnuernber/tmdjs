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
