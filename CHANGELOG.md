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
