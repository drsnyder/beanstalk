(ns com.github.drsnyder.stream
  (:refer-clojure :exclude [read read-line send])
  (:use [clojure.java.io])
  (import [java.io InputStream
                   BufferedInputStream
                   ByteArrayOutputStream]
          [java.nio ByteBuffer]))

(def #^String ^:dynamic *string-charset* "UTF-8")

(def #^Boolean ^:dynamic *return-byte-arrays?* false)

 
(def CR (byte 0x0d))
(def LF (byte 0x0a))
(def CRLF (byte-array 2 [CR LF]))

(defn- CR? [b] (= b CR))
(defn- LF? [b] (= b LF))

(defn- read-expected-byte [#^InputStream stream expected]
  (let [actual (.read stream)
        expected (int expected)]
    (cond
      (= actual expected) actual
      (= actual -1) (throw (Exception. "End of stream reached"))
      true (throw (Exception. (format "Expected byte: 0x%1$x, read 0x%2$x" expected actual))))))

(defprotocol ProtocolStream
  (#^String read [stream count])
  (#^String read-line [stream]))

(extend-type BufferedInputStream
  ProtocolStream
  (read [this count]
    (let [buf (byte-array count)]
      (loop [start 0]
        (let [nread (.read this buf start (- count start))]
          (when (= -1 nread)
            (throw (Exception. (str "End of stream reached"))))
          (if (= (+ nread start) count)
            (if *return-byte-arrays?*
              buf
              (String. buf *string-charset*))
            (recur (+ nread start)))))))

  (read-line [this]
    (let [buf (ByteArrayOutputStream.)]
      (loop [byte (.read this)]
        (when (= byte -1)
          (throw (Exception. "End of stream reached")))
        (if (CR? byte)
          (let [next (.read this)]
            (if (LF? next)
              (String. (.toByteArray buf) *string-charset*)
              (throw (Exception. "Read CR not followed by LF"))))
          (do
            (.write buf byte)
            (recur (.read this))))))))
