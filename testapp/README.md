# Test App for tmdjs

## Usage

1. Create package.json - `clj -M:cljs-install -m cljs.main --install-deps`.
2. `npm install`
3. `clj -M:cljs compile app`

Now you can run the server from the main namespace and check out the timings.

In order to run the cljs repl use

1. `clj -M:cljs watch app`
2. cider-connect to port 8777
3. `(shadow/repl :app)`

To test a release version type:

* `rm -rf resources/public && clj -M:cljs release app`
