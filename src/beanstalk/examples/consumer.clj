(ns beanstalk.examples.consumer
  (:gen-class)
  (:use clojure.contrib.command-line)
  (:use beanstalk.core))

; .clojure
; ./src:./classes:./lib:beanstalk-1.0.0-SNAPSHOT.jar
; (compile 'beanstalk.examples.consumer)
; java -cp ./classes/:./lib/clojure-1.2.0.jar:./lib/clojure-contrib-1.2.0.jar beanstalk.examples.consumer
;
; simpler:
; lein run -m beanstalk.examples.consumer

(defn -main [& args]
  (with-command-line args
                     "Beanstalk simple consumer process"
                     [[host "The host to connect to" "localhost"]
                      [port "This is the description for bar" 11300]
                      [tube "The tube to \"use\"." "beanstalk-clj-test"]
                      remaining]

                     (println (str "Using host " host " port " port " and tube " tube))

                     (with-open [b (new-beanstalk host port)]
                       (let [ret (.watch b tube)]
                         (if ret
                           (loop [job (.reserve b)]
                             (if (not (= (:payload job) "exit"))
                               (do
                                 (println "=> " (:payload job))
                                 (.delete b (:id job))
                                 (recur (.reserve b)))
                               (println "Exiting.")))
                           (println (str "Error, watch failed: " ret)))))))
