(ns com.github.drsnyder.beanstalk.examples.producer
  (:gen-class)
  (:refer-clojure :exclude [read peek use])
  (:use clojure.tools.cli)
  (:use com.github.drsnyder.beanstalk))

; .clojure
; ./src:./classes:./lib:beanstalk-1.0.0-SNAPSHOT.jar
; (compile 'com.github.drsnyder.beanstalk.examples.producer)
; java -cp ./classes/:./lib/clojure-1.2.0.jar:./lib/clojure-contrib-1.2.0.jar com.github.drsnyder.beanstalk.examples.producer
;
; simpler:
; lein run -m com.github.drsnyder.beanstalk.examples.producer

(defn -main [& args]
  (let [[opts args _] (cli args
                           #_"Beanstalk simple producer"
                           ["--host" "The host to connect to" 
                                     :default "localhost"]
                           ["--port" "This is the description for bar" 
                                     :default 11300
                                     :parse-fn #(Integer. %)]
                           ["--tube" "The tube to \"use\"." 
                                     :default "beanstalk-clj-test"]
                           ["-m" "--message" "The message to send" 
                                             :default "hello"]
                           ["-n" "--iterations" "The number of iterations" 
                                                :default 5
                                                :parse-fn #(Integer. %)])
        host          (:host opts)
        port          (:port opts)
        tube          (:tube opts)
        message       (:message opts)
        iterations    (:iterations opts)]
    (println (str "Using host " host " port " port " and tube " tube))
    (with-open [b (new-beanstalk host port)]
      (let [ret (.use b tube)]
        (if ret
          (loop [count iterations]
            (when (> count 0)
              (.put b 0 0 0 (.length message) message)
              (recur (dec count))))
          (println (str "Error, watch failed: " ret)))))))

