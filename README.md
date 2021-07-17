# DataFrame and Numerics for ClojureScript

## Development

This is what I have so far to make development quick

### Get a REPL

* clj -M:cljs node-repl
* cider-connect to port 8777 once it starts
* `(shadow/repl :node-repl)`

### Unit Tests

There is a test script - `scripts/run-tests` that does:

* clj -M:cljs compile app
* node target/test.js
