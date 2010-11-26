(ns beanstalk.core

  (use clojure.java.io)
  (import [java.io BufferedReader]))

;
;  (use 'clojure.java.io)
;  (import '[java.io BufferedReader])

(def *debug* false)

(defstruct Beanstalk :socket :reader :writer)
(def bsock (accessor Beanstalk :socket))
(def breader (accessor Beanstalk :reader))
(def bwriter (accessor Beanstalk :writer))

;(defn conn-new 
;  ([port] (java.net.Socket. "localhost" port))
;  ([host port] (java.net.Socket. host port)))

(defn create [host port]
  (let [s (java.net.Socket. "localhost" port)]
    (struct-map Beanstalk
                :socket s
                :reader (reader s)
                :writer (writer s)
                )))

(defn beanstalk
  ([port] (create "localhost" port))
  ([host port] (create host port))
  ([] (create "localhost" 11300)))

(defn beanstalk-close [b]
  (.close (bsock b)))

(defn beanstalk-write [b msg]
  (let [w (bwriter b)]
    (do 
      (. w write msg) 
      (. w flush))) b)


(defn beanstalk-read [b]
  (let [r (breader b) 
        sb (StringBuilder.)]
    (loop [c (.read r)]
      (cond 
        (neg? c) (str sb)
        (and (= \newline (char c)) 
             (> (.length sb) 1) 
             (= (char (.charAt sb (- (.length sb) 1) )) \return))
              (str (.substring sb 0 (- (.length sb) 1)))
        true (do (.append sb (char c))
               (recur (.read r)))))))

(defn beanstalk-read [b]
  (binding [*in* (breader b)]
    (read-line)))


(def b (beanstalk 11300))
(beanstalk-write b "stats\r\n") 
(beanstalk-read b) 

