(ns com.github.drsnyder.beanstalk.examples.consumer
  (:gen-class)
  (:refer-clojure :exclude [read peek use])
  (:use clojure.tools.cli)
  (:use com.github.drsnyder.beanstalk))

; .clojure
; ./src:./classes:./lib:beanstalk-1.0.0-SNAPSHOT.jar
; (compile 'com.github.drsnyder.beanstalk.examples.consumer)
; java -cp ./classes/:./lib/clojure-1.2.0.jar:./lib/clojure-contrib-1.2.0.jar com.github.drsnyder.beanstalk.examples.consumer
;
; simpler:
; lein run -m com.github.drsnyder.beanstalk.examples.consumer

(defn -main [& args]
  (let [[opts args _] (cli args
                           #_"Beanstalk simple consumer process"
                           ["--host" "The host to connect to" 
                                     :default "localhost"]
                           ["--port" "This is the port to connect to"
                                     :default  11300 
                                     :parse-fn #(Integer. %)]
                           ["--tube" "The tube to \"use\"." 
                                     :default "beanstalk-clj-test"])
        host          (:host opts)
        port          (:port opts)
        tube          (:tube opts)]
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
