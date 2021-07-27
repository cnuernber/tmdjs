# DataFrame and Numerics for ClojureScript

[![Clojars Project](https://img.shields.io/clojars/v/com.cnuernber/tmdjs.svg)](https://clojars.org/com.cnuernber/tmdjs)

Minimal cljs implementation of `tech.v3.datatype`, `tech.v3.datatype.functional`,
`tech.v3.datatype.argops`, and `tech.v3.dataset`.  This implementation is based
on typed-arrays for numeric data and js arrays for everything else so it should
support all your favorite datatypes.  Support for `java.time.Instant` and
`java.time.LocalDate` is included.

Datasets serialize and deserialize much faster than sequences of maps.  They use less
memory and they allow faster columnwise operations.  To transform a sequence of maps
into a dataset use `ds/->dataset`.  To get a sequence of maps back use `ds/rows`.


This library is mainly going to useful if you are dealing with large amounts of primarily
numeric data such as timeseries data coming off of a sensor.  In that case you can specify
exactly the datatype of the column which will get you major benefits in terms of
memory and serialization size.  I developed this library when working with such data
in a react-native application.


Unlike the jvm-version this is a very minimal exposition of these concepts.  Since the
underlying vm itself is typeless there was no need for a complex macro system to do
unboxed math in loops so I could stay much closer to core clojure and in fact ICounted
and IIndexed are the primary interfaces and `tech.v3.datatype/reify-reader` creates a
persistent-vector hash and equiv compatible object.


## Example


#### Server Side

There is a new namespace, tech.v3.libs.transit that contains a transit-safe
dataset->data function.  There are also transit handlers defined if you know how to
override your transit handlers in your middleware.

```clojure
(defn generate-data
  [request]
  (-> (ds/->dataset (repeatedly 10000 #(hash-map :time (rand)
                                                 :temp (rand)
                                                 :temp1 (rand)
                                                 :temp2 (rand)
                                                 :valid? (if (> (rand) 0.5)
                                                           true
                                                           false))))
      (ds-t/dataset->data)
      (response/response)))
```

#### Client Side

The same namespace tech.v3.dataset is exposed for clojurescript containing most of the
usual functions - columns, rows, select-rows, group-by, etc.  The version of
dataset->data in this namespace corresponds with the version on the jvm side above.
There are also transit handlers defined if you have access to override the transit
handlers in your middleware stack.

```clojure
  (GET "/data" {:handler #(let [ds (ds/data->dataset %)]
                            (swap! app* assoc
                                   :ds ds
                                   :raw (mapv (partial into {}) (ds/rows ds))))})
```

* See [testapp](testapp) for a minimal quick walkthrough and verification that
advanced optimizations do not break the api.


## Development

This is what I have so far to make development quick

### Get a REPL

* clj -M:cljs node-repl
* cider-connect to port 8777 once it starts
* `(shadow/repl :node-repl)`

### Unit Tests

There is a test script - `scripts/run-tests` that does:

* clj -M:cljs compile test
* node target/test.js

### Install locally and try on different project

* scripts/install-local

### License

* MIT
