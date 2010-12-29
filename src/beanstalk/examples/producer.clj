(ns beanstalk.examples.producer
  (:gen-class)
  (:refer-clojure :exclude [read peek use])
  (:use clojure.contrib.command-line)
  (:use beanstalk.core))

; .clojure
; ./src:./classes:./lib:beanstalk-1.0.0-SNAPSHOT.jar
; (compile 'beanstalk.examples.producer)
; java -cp ./classes/:./lib/clojure-1.2.0.jar:./lib/clojure-contrib-1.2.0.jar beanstalk.examples.producer
;
; simpler:
; lein run -m beanstalk.examples.producer

(defn -main [& args]
  (with-command-line args
                     "Beanstalk simple producer"
                     [[host "The host to connect to" "localhost"]
                      [port "This is the description for bar" 11300]
                      [tube "The tube to \"use\"." "beanstalk-clj-test"]
                      [message m "The message to send" "hello"]
                      [iterations n "The number of iterations" 5]
                      remaining]

                     (println (str "Using host " host " port " port " and tube " tube))

                     (with-open [b (new-beanstalk host port)]
                       (let [ret (.use b tube)]
                         (if ret
                           (loop [count (Integer. iterations)]
                             (when (> count 0)
                               (.put b 0 0 0 (.length message) message)
                             (recur (dec count))))
                           (println (str "Error, watch failed: " ret)))))))

