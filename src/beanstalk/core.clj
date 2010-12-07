(ns beanstalk.core

  (require clojure.contrib.condition)
  (require clojure.string)
  (use clojure.java.io)
  (import [java.io BufferedReader]))


;(use 'clojure.contrib.condition)
;(use 'clojure.string)
;(use 'clojure.java.io)
;(import '[java.io BufferedReader])

(def *debug* false)
(def *crlf* (str \return \newline))


(defn beanstalk-debug [msg]
  (when *debug* (println msg)))

; translates to
(defmacro cmd-reply-case [req clauses]
  `(let [reply# (parse-reply ~req)]
    (beanstalk-debug (str "* <= " reply#))
     (condp = (:response reply#)
       ~@clauses
       (clojure.contrib.condition/raise 
          :message (str "Unexpected response from sever: " (:response reply#))))))


(defn beanstalk-cmd [s & args]
    (if (nil? args)
      (str (name s) \return \newline)
      (str (name s) " " (.concat (str (reduce #(str %1 " " %2) args)) 
                          (str \return \newline)))))

; type conversion might be (Ingeger. var)
(defn parse-reply [reply]
  (let [parts (clojure.string/split reply #"\s+")
        response (keyword (clojure.string/lower-case (first parts)))
        data (reduce #(str %1 " " %2) (rest parts))]
    {:response response :data data}))



(defn stream-write [w msg]
    (do (. w write msg) (. w flush)))

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
    (stats [this] (do (stream-write writer (beanstalk-cmd :stats)) 
                      (cmd-reply-case (stream-read reader) 
                                      (:ok (stream-read reader))))))

(defn Beanstalk-create 
  ([] (let [s (java.net.Socket. "localhost" 11300)]
                (Beanstalk. s (reader s) (writer s))))  
  ([port] (let [s (java.net.Socket. "localhost" port)]
                (Beanstalk. s (reader s) (writer s))))
  ([host port] (let [s (java.net.Socket. host port)]
                (Beanstalk. s (reader s) (writer s)))))


;(def B (Beanstalk))
;(.stats B)
