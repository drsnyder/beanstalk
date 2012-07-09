(ns com.github.drsnyder.repl-helper
  (:refer-clojure :exclude [read read-line send])
  (:use [com.github.drsnyder stream]
        [lamina.core] 
        [aleph.tcp]
        [gloss.core])
  (import [java.io InputStream
                   BufferedInputStream
                   ByteArrayOutputStream]
          [java.nio ByteBuffer])
  (:require [clojure.test :as testing]))

(defn beanstalk-cmd [s & args]
    (if (nil? args)
      (name s) 
      (str (name s) " " (str (reduce #(str %1 " " %2) args)))))

(defn to-ascii [value] 
  (.getBytes (str value) "ASCII"))

(defn put-bytes [#^ByteArrayOutputStream buf #^bytes bytes]
  (.write buf bytes 0 (alength bytes)))

(defn beanstalk-data [data]
  (str data))

(def create-conn 
  #(deref (tcp-client {:host "localhost", :port 11300, :frame (string :utf-8 :delimiters ["\r\n"])})))

(def bch (create-conn))

(defn put [channel pri del ttr payload]
  (do
    (enqueue channel (beanstalk-cmd :put pri del ttr (.length payload))) 
    (enqueue channel payload)
    (read-channel channel)))

(defn put [channel pri del ttr payload]
  (do
    (enqueue channel (beanstalk-cmd :put pri del ttr (.length payload))) 
    (enqueue channel payload)
    ; confirm INSERTED
    @(read-channel channel)))

(defn reserve [channel]
  (do
    (enqueue channel (beanstalk-cmd :reserve))
    (let [response @(read-channel channel)]
      (if-let [reserved? (re-matches #"RESERVED \d+ \d+" response)]
        @(read-channel channel)
        (throw (Exception. (format "Error reserving: %" response)))))))
