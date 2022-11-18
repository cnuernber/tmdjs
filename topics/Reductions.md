# Some Reduction Timings


The datatype library has some helpers that work with datasets that can make certain types of
reductions much faster.


## Filter On One Column, Reduce Another

This is a very common operation so let's take a closer look.  The generic dataset
pathway would be:

```clojure
cljs.user> (require '[tech.v3.dataset :as ds])
nil
cljs.user> (def test-ds (ds/->dataset {:a (range 20000)
                                       :b (repeatedly 20000 rand)}))
#'cljs.user/test-ds
cljs.user> ;;filter on a, sum b.
cljs.user> (reduce + 0.0 (-> (ds/filter-column test-ds :a #(> % 10000))
                             (ds/column :b)))
5000.898384571656
cljs.user> (time (dotimes [idx 100] (reduce + 0.0 (-> (ds/filter-column test-ds :a #(> % 10000))
                                                      (ds/column :b)))))
"Elapsed time: 282.714231 msecs"
"Elapsed time: 282.714231 msecs"
"Elapsed time: 282.714231 msecs"
```

Think transducers are fast?  What about a generic transducer pathway?

```clojure
cljs.user> (let [a (test-ds :a)
                 b (test-ds :b)]
             (transduce (comp (filter #(> (nth a %) 10000))
                              (map #(nth b %)))
                        (completing +)
                        (range (ds/row-count test-ds))))
5000.898384571656
cljs.user> (time (dotimes [idx 100] 
                   (let [a (test-ds :a)
                         b (test-ds :b)]
                     (transduce (comp (filter #(> (nth a %) 10000))
                                      (map #(nth b %)))
                                (completing +)
                                (range (ds/row-count test-ds))))))
"Elapsed time: 436.235972 msecs"
"Elapsed time: 436.235972 msecs"
"Elapsed time: 436.235972 msecs"
nil
```

Transducers are fast - after looking at this pathway we found the
`nth` call is relatively expensive.  The datatype library has a way
to get the fastest nth-like access available for a given container.  Columns overload
this pathway such that if there are no missing they use the fastest
access for their buffer, else they have to wrap a missing check.  Regardless,
this gets us a solid improvement:

```clojure
cljs.user> (require '[tech.v3.datatype :as dtype])
nil
cljs.user> (time (dotimes [idx 100] 
                   (let [a (dtype/->fast-nth (test-ds :a))
                         b (dtype/->fast-nth (test-ds :b))]
                     (transduce (comp (filter #(> (a %) 10000))
                                      (map #(b %)))
                                (completing +)
                                (range (ds/row-count test-ds))))))
"Elapsed time: 77.823553 msecs"
"Elapsed time: 77.823553 msecs"
"Elapsed time: 77.823553 msecs"
nil
```

OK - there is another more dangerous approach.  dtype has another query,
as-agetable, that either returns something for which `aget` works or
nil.  If you know your dataset's columns have no missing data and their
backing store data itself is agetable - then you can get an agetable.  This
doesn't have a fallback so you risk null ptr issues - but it is the fastest
possible pathway.


```clojure
cljs.user> (time (dotimes [idx 100] 
                   (let [a (dtype/as-agetable (test-ds :a))
                         b (dtype/as-agetable (test-ds :b))]
                     (transduce (comp (filter #(> (aget a %) 10000))
                                      (map #(aget b %)))
                                (completing +)
                                (range (ds/row-count test-ds))))))
"Elapsed time: 57.404783 msecs"
"Elapsed time: 57.404783 msecs"
"Elapsed time: 57.404783 msecs"
nil
```


In this simple example we find that a transducing pathway is indeed a quite bit faster but only 
when it is coupled with an efficient per-element access pattern.
