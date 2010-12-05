(ns beanstalk.core

  (use clojure.contrib.condition)
  (use clojure.java.io)
  (import [java.io BufferedReader]))

;
  (use clojure.contrib.condition)
  (use 'clojure.java.io)
  (import '[java.io BufferedReader])

(def *debug* false)
(def *crlf* (str \return \newline))

(defprotocol BeanstalkObject
  (close [this] "Close the connection")
  (stats [this] "stats command")
  (write [this msg] "Write to the socket using the writer")
  (read [this] "Read from the socket using the reader")
  ;(put [this pri del ttr length data] "put command")
  ;(use [this tube] "use command")
  ;(reserve [this] "reserve command")
             )

(defrecord Beanstalk [socket reader writer]
  BeanstalkObject
    (close [this] (.close socket))
    (write [this msg] (stream-write writer (beanstalk-cmd msg)))
    (read [this] (stream-read reader))
    (stats [this] (do 
                    (.write this (beanstalk-cmd :stats))
                    (.read this)    ; ok
                    (.read this)))) ; payload



;(cmd-reply-case (stream-write writer (beanstalk-cmd "stats"))
;  (:ok (stream-read reader)))

; cmd = "stats", symbol = :ok, fn = (stream-read reader)
;(cmd-reply-case writer cmd
;  (symbol fn))

; translates to
(defmacro cmd-reply-case [req clauses]
  `(let [reply# ~req]
     (condp = (:response reply#)
       ~@clauses
       (raise :message (str "Unexpected response from sever: " response)))))

(macroexpand `(cmd-reply-case (stream-write writer (beanstalk-cmd :stats))
  (:ok (stream-read reader))))

;(let [reply (stream-write writer (beanstalk-cmd (name cmd)))]
;  (cond = (:response reply)
;    :ok fn
;    (raise :message (str "Unexpected response from sever: " response)))
;  )

(defn Beanstalk-create [host port]
  (let [s (java.net.Socket. "localhost" port)]
    (Beanstalk. s (reader s) (writer s))))


(defn beanstalk-cmd [s & args]
    (if (nil? args)
      (str (name s) \return \newline)
      (str (name s) " " (.concat (str (reduce #(str %1 " " %2) args)) 
                          (str \return \newline)))))

; type conversion might be (Ingeger. var)
(defn parse-reply [reply]
  (let [parts (split reply #"\s+")
        response (keyword (clojure.string/lower-case (first parts)))
        data (reduce #(str %1 " " %2) (rest parts))]
    {:response response :data data}))

; keep
(defn Beanstalk
  ([port] (Beanstalk-create "localhost" port))
  ([host port] (Beanstalk-create host port))
  ([] (Beanstalk-create "localhost" 11300)))


(defn stream-write [w msg]
    (do (. w write msg) (. w flush)))

(defn stream-read [r]
  (binding [*in* r]
    (read-line)))


(def B (Beanstalk-create "localhost" 11300))
(.stats B)
