# 2.001
 * Fix many issues that had crept in - one being (seq ds) being a sequence of keyvals which is wrong - java TMD implements seq/reduce as sequence of columns.  **This leads to other incompatilibities mainly keys/vals no longer work!!.**.
 * `(assoc ds :b (dt/reify-reader ...))` works correctly both when using :object datatype and and exact datatype - this is a lazy operation and does not rescan the data for ##NaN and friends -- if you want to provide a missing values 
   using the form `#:tech.v3.dataset{:data (reify-reader...) :missing #{missing data}}`.  This form works with assoc.  As always passing in a container with `:object` datatype will always force a scan of the data.
   
# 2.000
 * Latest TMD support - clj-transit is now in TMD and not in this project as transit is a first-class format for tmd.
 * clojars coord is now com.cnuernber/tmdjs to match other libs.
 
# 2.000-beta-10
 * Added high performance pathway for parsing datasets and realizing intermediate datasets.
   The dataset-parser now implements several protocols including count, nth, so you can get,
   for instance, the last row during the parsing phase.  When using the dataset-parser you
   have to use the tech.v3.dataset.protocols/-add-row and -add-rows methods.
# 2.000-beta-9
* fast-nth fixed for integer columns.
* various fixes around parsing data.
