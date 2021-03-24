(ns bd2.core)
(require '[clojure.java.jdbc :as sql])
(require '[clj-time.core :as t])
(require '[clj-time.local :as l])
(require '[clojure.data.csv :as csv]
         '[clojure.java.io :as io])

(def db-spec "postgresql://postgres:123456@localhost:5432/bd2")

(def EXPLICITY_SAVE true)
(def EXPLICITY_NOT_SAVE false)
(def ERROR true)
(def NOT_ERROR false)

(defn clear-db [] (sql/execute! db-spec ["DELETE FROM product WHERE True"]))

(defn count-db [] (sql/query db-spec ["SELECT COUNT (eid) FROM product WHERE True;"]))

(defn parse-csv []
  (defn csv-data->maps [csv-data]
    (map zipmap
         (->> (first csv-data) ;; First row is the header
              (map keyword) ;; Drop if you want string keys instead
              repeat)
         (rest csv-data)))

  (def x (with-open [reader (io/reader "data.csv")]
           (doall
            (csv-data->maps (csv/read-csv reader)))))
  x)

(def x (parse-csv))

(defn build-sql [id, product]
  {:eid id :description product})


(defn implicity [error]
  (def queries (map-indexed (fn [index item] (build-sql index (item :product))) x))

  (def queries-error [{:eid 50000 :description "Teste Passou"},
                      {:eid "Teste Falhou" :description "Teste Falhou"}])


  (def values (if error queries-error queries))

  (def start-time (l/local-now))

  (doseq [query values] (sql/insert! db-spec :product query))

  (def end-time (l/local-now))

  (def milliseconds (t/in-millis (t/interval start-time end-time)))

  (format "Total time, %s ms." milliseconds))

(defn explicity [save, error]
  (def queries-multi (map-indexed (fn [index item] [(item :product), index]) x))
  (def queries-error [
    ["testePassou", "50000"]
    ["testeFalhou", "idInvalido"]])

  
  (def values (if error queries-error queries-multi))

  (sql/with-db-transaction [t-con db-spec]
    (if (not save)
      (sql/db-set-rollback-only! t-con))

    (def start-time (l/local-now))

    (sql/insert-multi! t-con :product
                       [:description :eid]
                       values)

    (def end-time (l/local-now))

    (def milliseconds (t/in-millis (t/interval start-time end-time)))

    (format "Total time, %s ms." milliseconds)))


(defn -main []
  ;; (use '(bd2.core))
  ;; (clear-db)
  ;; (count-db)
  ;; not commit changes, do not save
  ;; (explicity EXPLICITY_NOT_SAVE NOT_ERROR)
  ;; saves on db
  ;; (explicity EXPLICITY_SAVE NOT_ERROR)
  ;; 
  ;; (explicity EXPLICITY_NOT_SAVE ERROR)
  ;; saves on db/
  ;; (explicity EXPLICITY_SAVE ERROR)
  ;; 
  ;; (implicity ERROR)
  ;; (implicity NOT_ERROR)
  ;;   
)
