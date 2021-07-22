(ns tech.v3.datatype.format-sequence
  "Format a sequence of numbers.  We do what we can here...")


(defn- left
  "What is the power of number"
  ^long [^double x]
  (-> x Math/abs Math/log10 Math/floor unchecked-long inc))

(defn- right
  "Calculate maximum digits on the right side of the dot."
  ^long [^double x ^long max-digits]
  (let [orig-x (Math/abs x)]
    (loop [n-pow 1.0
           idx 0]
      (let [x (* orig-x n-pow)]
        (if (or (>= idx max-digits)
                (= (Math/floor x) x))
          idx
          (recur (* n-pow 10.0)
                 (unchecked-inc idx)))))))

;; public functions

(defn formatter
  "Create formatter for given:
  * `xs` - sequence of doubles
  * `digits` - maximum precision
  * `threshold` - what is absolute power to switch to scientific notation
  Returns formatter."
  ([xs] (formatter xs 8))
  ([xs ^long digits] (formatter xs digits 8))
  ([xs ^long digits ^long threshold]
   (let [xs (filter #(and (number? %) (js/isFinite %)) xs)
         max-left (apply max 0 (map left xs))
         max-right (apply max 0 (map #(right % digits) xs))
         e? (> max-left threshold)
         format-fn (if e?
                     #(.toExponential ^double %)
                     #(.toFixed ^double % max-right))]
     (fn [x]
       (let [^double x (js/parseFloat (or x ##NaN))]
         (if (js/isFinite x)
           (format-fn x)
           (cond
             (== ##Inf x) "Inf"
             (== ##-Inf x) "-Inf"
             :else "NaN")))))))


(defn format-sequence
  "Format sequence of double for given:
  * `xs` - sequence of doubles
  * `digits` - maximum precision
  * `threshold` - what is absolute power to switch to scientific notation
  Returns sequence of strings."
  ([xs] (format-sequence xs 8))
  ([xs ^long digits] (format-sequence xs digits 8))
  ([xs ^long digits ^long threshold]
   (let [fmt (formatter xs digits threshold)]
     (map fmt xs))))
