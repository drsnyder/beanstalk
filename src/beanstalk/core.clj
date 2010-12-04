(ns beanstalk.core

  (use clojure.java.io)
  (import [java.io BufferedReader]))

;
;  (use 'clojure.java.io)
;  (import '[java.io BufferedReader])

(def *debug* false)
(def *crlf* (str \return \newline))

(defprotocol BeanstalkObject
  (close [this] "Close the connection")
  (stats [this] "stats command")
  ;(put [this pri del ttr length data] "put command")
  ;(use [this tube] "use command")
  ;(reserve [this] "reserve command")
             )

(defrecord Beanstalk [socket reader writer]
  BeanstalkObject
    (close [this] (.close socket))
    (stats [this] (do 
                    (Beanstalk-write writer (beanstalk-cmd "stats"))
                    (Beanstalk-read reader)    ; ok
                    (Beanstalk-read reader)))) ; payload

(cmd-reply-case (Beanstalk-write writer (beanstalk-cmd "stats"))
  ; this should translate into a case
  (:ok (Beanstalk))
 )

;(defstruct Beanstalk :socket :reader :writer)
;(def bsock (accessor Beanstalk :socket))
;(def breader (accessor Beanstalk :reader))
;(def bwriter (accessor Beanstalk :writer))

;(defn conn-new 
;  ([port] (java.net.Socket. "localhost" port))
;  ([host port] (java.net.Socket. host port)))

(defn Beanstalk-create [host port]
  (let [s (java.net.Socket. "localhost" port)]
    (Beanstalk. s (reader s) (writer s))))

;(defn- beanstalk-create [host port]
;  (let [s (java.net.Socket. "localhost" port)]
;    (struct-map Beanstalk
;                :socket s
;                :reader (reader s)
;                :writer (writer s)
;                )))

(defn beanstalk-cmd [s & args]
    (if (nil? args)
      (str s \return \newline)
      (str s " " (.concat (str (reduce #(str %1 " " %2) args)) 
                          (str \return \newline)))))

; type conversion might be (Ingeger. var)
(defn parse-reply [reply]
  (let [parts (split reply #"\s+")
        response (keyword (lower-case (first parts)))
        data (reduce #(str %1 " " %2) (rest parts))]
    {:response response :data data}))

; keep
;(defn Beanstalk
;  ([port] (Beanstalk-create "localhost" port))
;  ([host port] (Beanstalk-create host port))
;  ([] (Beanstalk-create "localhost" 11300)))

;(defn beanstalk
;  ([port] (beanstalk-create "localhost" port))
;  ([host port] (beanstalk-create host port))
;  ([] (beanstalk-create "localhost" 11300)))

;(defn beanstalk-close [B]
;  (.close (:socket B)))

(defn Beanstalk-write [w msg]
    (do (. w write (beanstalk-cmd msg)) (. w flush)))

;(defn beanstalk-write [b msg]
;  (let [w (bwriter b)]
;    (do (. w write (beanstalk-cmd msg)) (. w flush))) b)

(defn Beanstalk-read [r]
  (binding [*in* r]
    (read-line)))

;(defn beanstalk-read [b]
;  (binding [*in* (breader b)]
;    (read-line)))

; handly reply gracefully
;(defn reply)
;
;; what about retry?
;(defn put [b pri del ttr bytes data]
;  (if (reply (beanstalk-write (beanstalk-write b (beanstalk-cmd "put" pri del ttr bytes)) data))
;    b
;    false))

;(def b (beanstalk 11300))
;(beanstalk-write b "stats") 
;(beanstalk-read b) 

(def B (Beanstalk-create "localhost" 11300))
(.stats B)
