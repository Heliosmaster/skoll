(ns index
  (:require
    [clojure.string :as str]
    [nextjournal.clerk :as clerk]
    [clojure.data.csv :as csv]
    [clojure.java.io :as io]
    [clojure.set :as set]
    ))

(comment
  (clerk/serve! {:browse? true})
  )

(def key->labels
  {:pun-price-f1 "Prezzo PUN F1  (cEuro/kWh)"
   :pun-price-f2 "Prezzo PUN F2  (cEuro/kWh)"
   :pun-price-f3 "Prezzo PUN F3  (cEuro/kWh)"
   :pun-price-f4 "Prezzo PUN F4  (cEuro/kWh)"
   :consumption "Misura energia prelevata netta per misura monoraria(kWh)"
   :consumption-f1 "Misura energia prelevata  F1 (kWh)"
   :consumption-f2 "Misura energia prelevata  F2 (kWh)"
   :consumption-f3 "Misura energia prelevata  F3 (kWh)"
   :consumption-f4 "Misura energia prelevata  F4 (kWh)"
   :pun-average-monthly-price "PUN medio mensile (cEuro/kWh)"
   :oe-value "Valore OE (Euro)"

   :average-monthly-zone-price "Prezzo medio mensile zonale (cEuro/kWh)"
   :gross-production "Misura energia immessa lorda misura monoraria (kWh)"
   :gross-production-f1 "Misura energia immessa lorda F1 (kWh)"
   :gross-production-f2 "Misura energia immessa lorda F2 (kWh)"
   :gross-production-f3 "Misura energia immessa lorda F3 (kWh)"
   :gross-production-f4 "Misura energia immessa lorda F4 (kWh)"
   :net-production-f1 "Misura energia immessa netta F1 (kWh)"
   :net-production-f2 "Misura energia immessa netta F2 (kWh)"
   :net-production-f3 "Misura energia immessa netta F3 (kWh)"
   :net-production-f4 "Misura energia immessa netta F4 (kWh)"
   :net-production "Misura energia immessa netta per misura monoraria(kWh)"
   :loss-coefficient-ki "Coefficiente perdita ki"
   :zone-price-f1 "Prezzo fascia F1 zonale (cEuro/kWh)"
   :zone-price-f2 "Prezzo fascia F2 zonale (cEuro/kWh)"
   :zone-price-f3 "Prezzo fascia F3 zonale (cEuro/kWh)"
   :tension-level "Livello di tensione"})

(def labels->keys (set/map-invert key->labels))

(defn with-pretty-label [a-map]
  (into {} (map (fn [[k v]]
                  [(key->labels k) v])
                a-map)))

(defn parse-italian-double [s]
  (or (parse-double (str/replace s #"," "."))
      0))

(def months ["January" "February" "March" "April" "May" "June" "July" "September" "October" "November" "December"])

(def process-row (juxt first (comp vec rest)))

(defn process-part [part]
  (->> (apply mapv vector part)

       (map process-row)
       (map (fn [[label vals]]
              (when-let [k (labels->keys label)]
                [k (mapv parse-italian-double vals)])))
       (into {})))

(defn read-csv [year]
  (with-open [reader (io/reader (str "data/" year ".csv"))]
    (let [raw (->> (doall (csv/read-csv reader :separator \;))
                   (drop-last 1) ;; remove weird EOF char
                   (partition-by #(= % [""]))
                   (remove (partial every? #(= % [""]))))
          consumption (first raw)
          production (last raw)]
      #_{:header (first raw)
         :total (nth raw 3)}
      (merge (process-part consumption)
             (process-part production))
      )))



(defn plot-production-consumption [d]
  (clerk/plotly {:data [{:x months
                         :type "bar"
                         :name "Consumption"
                         :y (:consumption d)}
                        {:x months
                         :type "bar"
                         :name "Production"
                         :y (:net-production d)}]}))



;; # 2013
(plot-production-consumption (read-csv 2013))
;; # 2014
(plot-production-consumption (read-csv 2014))
;; # 2015
(plot-production-consumption (read-csv 2015))
;; # 2016
(plot-production-consumption (read-csv 2016))
;; # 2017
(plot-production-consumption (read-csv 2017))
;; # 2018
(plot-production-consumption (read-csv 2018))
;; # 2019
(plot-production-consumption (read-csv 2019))
;; # 2020
(plot-production-consumption (read-csv 2020))
;; # 2021
(plot-production-consumption (read-csv 2021))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;











