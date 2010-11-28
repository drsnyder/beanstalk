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

(defn- beanstalk-create [host port]
  (let [s (java.net.Socket. "localhost" port)]
    (struct-map Beanstalk
                :socket s
                :reader (reader s)
                :writer (writer s)
                )))

(defn- beanstalk-cmd [s & args]
    (if (nil? args)
      (str s \return \newline)
      (str s " " (.concat (str (reduce #(str %1 " " %2) args)) 
                          (str \return \newline)))))

(defn beanstalk
  ([port] (beanstalk-create "localhost" port))
  ([host port] (beanstalk-create host port))
  ([] (beanstalk-create "localhost" 11300)))

(defn beanstalk-close [b]
  (.close (bsock b)))

(defn beanstalk-write [b msg]
  (let [w (bwriter b)]
    (do (. w write (beanstalk-cmd msg)) (. w flush))) b)

(defn beanstalk-read [b]
  (binding [*in* (breader b)]
    (read-line)))


;(def b (beanstalk 11300))
;(beanstalk-write b "stats") 
;(beanstalk-read b) 

