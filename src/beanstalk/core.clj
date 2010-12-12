(ns beanstalk.core

  (require clojure.contrib.condition)
  (require clojure.string)
  (require clojure.pprint)
  (use clojure.java.io)
  (import [java.io BufferedReader]))


;(use 'clojure.contrib.condition)
;(use 'clojure.string)
;(use 'clojure.java.io)
;(import '[java.io BufferedReader])

(def *debug* false)
(def *crlf* (str \return \newline))


(defn beanstalk-debug [msg]
  (when *debug* (clojure.pprint/pprint msg)))



(defn beanstalk-cmd [s & args]
    (if (nil? args)
      (str (name s) \return \newline)
      (str (name s) " " (.concat (str (reduce #(str %1 " " %2) args)) 
                          (str \return \newline)))))

(defn beanstalk-data [data]
  (str data \return \newline))

; type conversion might be (Integer. var)
(defn parse-reply [reply]
  (let [parts (clojure.string/split reply #"\s+")
        response (keyword (clojure.string/lower-case (first parts)))
        data (reduce #(str %1 " " %2) (rest parts))]
    {:response response :data data}))



(defn stream-write [w msg]
  (beanstalk-debug (str "* => " msg))
  (let [ret (. w write msg)]
    (. w flush) ret))

(defn stream-read [r]
  (let [sb (StringBuilder.)]
    (loop [c (.read r)]
      (cond 
        (neg? c) (str sb)
        (and (= \newline (char c)) 
             (> (.length sb) 1) 
             (= (char (.charAt sb (- (.length sb) 1) )) \return))
              (str (.substring sb 0 (- (.length sb) 1)))
        true (do (.append sb (char c))
               (recur (.read r)))))))

(defmacro cmd-reply-case
  ([response clauses]
   `(let [reply# (parse-reply ~response)]
      (beanstalk-debug (str "* <= " reply#))
      (or 
        (condp = (:response reply#)
          ~@clauses
          (clojure.contrib.condition/raise 
            :message (str "Unexpected response from sever: " (:response reply#)))) 
        (:data reply#))))
  ([request response clauses] `(do ~request (cmd-reply-case ~response ~clauses)))
  ([request data response clauses] `(do ~request ~data (cmd-reply-case ~response ~clauses))))


; handler => (fn [beanstalk reply] {:payload (.read beanstalk)})
; handler => (fn [beanstalk reply] {:payload (.read beanstalk) :id (Integer.  (:data reply))})
(defn protocol-response [beanstalk reply expected handler]
       (condp = (:response reply)
         expected (handler beanstalk reply)
         (clojure.contrib.condition/raise 
           :message (str "Unexpected response from sever: " (:response reply)))))

(defn protocol-case 
  ([beanstalk expected handle-response]
   (let [reply (parse-reply (.read beanstalk))]
     (beanstalk-debug (str "* <= " reply))
     (protocol-response beanstalk reply expected handle-response)))
  ([beanstalk cmd-str data expected handle-response]
   (do (.write beanstalk cmd-str)
     (.write beanstalk data)
     (protocol-case beanstalk expected handle-response)))
  ([beanstalk cmd-str expected handle-response]
   (do (.write beanstalk cmd-str)
     (protocol-case beanstalk expected handle-response))))

(defprotocol BeanstalkObject
  (close [this] "Close the connection")
  (read [this]  "Read from beanstalk")
  (write [this msg] "Write msg to beanstalk")
  (stats [this] "stats command")
  (put [this pri del ttr length data] "put command")
  (use [this tube] "use command")
  (reserve [this] "reserve command"))

(defrecord Beanstalk [socket reader writer]
           BeanstalkObject
           (close [this] (.close socket))
           (read [this] (stream-read reader))
           (write [this msg] (stream-write writer msg))
           (stats [this] 
                  (protocol-case 
                    this 
                    (beanstalk-cmd :stats) 
                    :ok 
                    (fn [b r] {:payload (.read b)})))
           (put [this pri del ttr length data] 
                (protocol-case 
                  this 
                  (beanstalk-cmd :put pri del ttr length)
                  (beanstalk-data data)
                  :inserted
                  (fn [b r] {:id (Integer. (:data r))})))
           (use [this tube] 
                (protocol-case 
                  this
                  (beanstalk-cmd :use tube)
                  :using
                  (fn [b r] (let [tube (:data r)] {:payload tube :tube tube}))))
           (reserve [this] 
                (protocol-case 
                  this
                  (beanstalk-cmd :reserve)
                  :reserved
                  (fn [b r] {:payload (.read b) 
                             ; response is "<id> <length>"
                             :id (Integer. (first (clojure.string/split (:data r) #"\s+")) )}))))
                    
                    ;(cmd-reply-case 
                    ;          (.write this (beanstalk-cmd :reserve)) 
                    ;          (.read this) 
                    ;          (:reserved false))))

(defn new-beanstalk
  ([host port] (let [s (java.net.Socket. host port)]
                 (Beanstalk. s (reader s) (writer s))))
  ([port]      (new-beanstalk "localhost" port))
  ([]          (new-beanstalk "localhost" 11300)))


;(def B (Beanstalk))
;(.stats B)
