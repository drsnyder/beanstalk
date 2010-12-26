(ns beanstalk.core

  (:use [clojure.contrib.condition :only [raise]]
        [clojure.string :only [split lower-case]]
        [clojure.pprint :only [pprint]]
        [clojure.java.io])
  (import [java.io BufferedReader]))


;(use 'clojure.contrib.condition)
;(use 'clojure.string)
;(use 'clojure.java.io)
;(import '[java.io BufferedReader])

(def *debug* false)
(def *crlf* (str \return \newline))


(defn beanstalk-debug [msg]
  (when *debug* (pprint msg)))


(defn beanstalk-cmd [s & args]
    (if (nil? args)
      (name s) 
      (str (name s) " " (str (reduce #(str %1 " " %2) args)))))


(defn beanstalk-data [data]
  (str data))


; type conversion might be (Integer. var)
(defn parse-reply [reply]
  (beanstalk-debug (str "parse-reply: " reply))
  (let [parts (split reply #"\s+")
        response (keyword (lower-case (first parts)))]
    (if (empty? (rest parts))
      {:response response :data nil}
      {:response response :data (reduce #(str %1 " " %2) (rest parts))})))


(defn stream-write [w msg]
  (beanstalk-debug (str "* => " msg))
  (do (. w write msg) 
    (. w write *crlf*)
    (. w flush)))


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


; handler => (fn [beanstalk reply] {:payload (.read beanstalk)})
; handler => (fn [beanstalk reply] {:payload (.read beanstalk) :id (Integer.  (:data reply))})
(defn protocol-response [beanstalk reply expected handler]
       (condp = (:response reply)
         expected (handler beanstalk reply)
         ; under what conditions do we retry?
         :expected_crlf (raise 
                          :type :expected_crlf
                          :message (str "Protocol error. No CRLF."))
         :not_found (raise 
                      :type :not_found
                      :message (str "Job not found."))
         :not_ignored false
         (raise 
           :type :protocol
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
  (use [this tube] "use command for producers")
  (watch [this tube] "watch command for consumers")
  (reserve [this] "reserve command")
  (reserve-with-timeout [this timeout] "reserve command")
  (delete [this id] "delete command")
  (release [this id pri del] "release command")
  (bury [this id pri] "bury command")
  (touch [this id] "touch command")
  (ignore [this tube] "ignore command")
  (peek [this id] "peek command")
  (peek-ready [this] "peek-ready command")
  (peek-delayed [this] "peek-delayed command")
  (peek-buried [this] "peek-buried command")
             )


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
           (watch [this tube] 
                (protocol-case 
                  this
                  (beanstalk-cmd :watch tube)
                  :watching
                  (fn [b r] {:count (Integer. (:data r))}))) 
           (reserve [this] 
                (protocol-case 
                  this
                  (beanstalk-cmd :reserve)
                  :reserved
                  (fn [b r] {:payload (.read b) 
                             ; response is "<id> <length>"
                             :id (Integer. (first (split (:data r) #"\s+")) )})))
           (reserve-with-timeout [this timeout] 
                (protocol-case 
                  this
                  (beanstalk-cmd :reserve-with-timeout timeout)
                  :reserved
                  (fn [b r] {:payload (.read b) 
                             ; response is "<id> <length>"
                             :id (Integer. (first (split (:data r) #"\s+")) )})))
           (delete [this id] 
                (protocol-case 
                  this
                  (beanstalk-cmd :delete id)
                  :deleted
                  (fn [b r] true)))
           (release [this id pri del] 
                (protocol-case 
                  this
                  (beanstalk-cmd :release id pri del)
                  :released
                  (fn [b r] true)))
           (bury [this id pri] 
                (protocol-case 
                  this
                  (beanstalk-cmd :bury id pri)
                  :buried
                  (fn [b r] true)))
           (touch [this id] 
                (protocol-case 
                  this
                  (beanstalk-cmd :touch id)
                  :touched
                  (fn [b r] true)))
           (ignore [this tube] 
                (protocol-case 
                  this
                  (beanstalk-cmd :ignore tube)
                  :watching
                  (fn [b r] {:count (Integer. (:data r))}))) 
           (peek [this id] 
                (protocol-case 
                  this
                  (beanstalk-cmd :peek id)
                  :found
                  (fn [b r] {:payload (.read b) 
                             ; response is "<id> <length>"
                             :id (Integer. (first (split (:data r) #"\s+")) )})))
           (peek-ready [this] 
                (protocol-case 
                  this
                  (beanstalk-cmd :peek-ready)
                  :found
                  (fn [b r] {:payload (.read b) 
                             ; response is "<id> <length>"
                             :id (Integer. (first (split (:data r) #"\s+")) )})))
           (peek-delayed [this] 
                (protocol-case 
                  this
                  (beanstalk-cmd :peek-delayed)
                  :found
                  (fn [b r] {:payload (.read b) 
                             ; response is "<id> <length>"
                             :id (Integer. (first (split (:data r) #"\s+")) )})))
           (peek-buried [this] 
                (protocol-case 
                  this
                  (beanstalk-cmd :peek-buried)
                  :found
                  (fn [b r] {:payload (.read b) 
                             ; response is "<id> <length>"
                             :id (Integer. (first (split (:data r) #"\s+")) )})))
           )

           
                    

(defn new-beanstalk
  ([host port] (let [s (java.net.Socket. host port)]
                 (Beanstalk. s (reader s) (writer s))))
  ([port]      (new-beanstalk "localhost" port))
  ([]          (new-beanstalk "localhost" 11300)))

