(ns beanstalk.core-test
  (:use beanstalk.core clojure.test))

; tracing: sudo tcpdump -i lo0 -tnNqA tcp port 11300 

(deftest test-beanstalk
         (let [b (new-beanstalk)]
           (is (and (not (nil? b)) 
                    (instance? beanstalk.core.Beanstalk b)))))

(deftest test-stats
         (binding [ beanstalk.core/*debug* true ]
           (let [b (new-beanstalk)
                 result (.stats b)]
             (is (not (nil? (:payload result))))
             (is (> (.length (:payload result)) 0)))))


(deftest test-put
         (binding [ beanstalk.core/*debug* true ]
           (let [b (new-beanstalk)
                 result (.put b 0 0 10 5 "hello")]
             (println (str "result => " result))
             (is (not (nil? result)))
             (println (str "put returned id " (:id result)))
             ; should be the job id
             (is (> (:id result) 0))
             (.delete b (:id result)))))


(deftest test-use
         (binding [ beanstalk.core/*debug* true ]
           (let [b (new-beanstalk)
                 result (.use b "test-tube")]
             (println (str "result => " result))
             (is (not (nil? result)))
             (is (= (:payload result) "test-tube")))))


(deftest test-reserve
         (binding [ beanstalk.core/*debug* true ]
           (let [p (new-beanstalk) ; producer
                 c (new-beanstalk) ; consumer
                 use-p (.use p "test-tube")
                 put-p (.put p 0 0 10 5 "hello")
                 watch-c (.watch c "test-tube")
                 reserve-c (.reserve c)]
             (println (str "result use => " use-p))
             (println (str "result put => " put-p))
             (println (str "result reserve => " reserve-c))
             (is (> (:count watch-c) 0))
             (is (not (nil? reserve-c)))
             (is (> (:id reserve-c) 0))
             (is (= (:payload reserve-c) "hello"))
             (is (= true (.delete c (:id put-p)))))))


;; make these tests more stable-- use a random tube?
(deftest test-reserve-with-timeout
         (binding [ beanstalk.core/*debug* true ]
           (let [data "hello-reserve-with-timeout"
                 p (new-beanstalk)
                 c (new-beanstalk)
                 use-p (.use p "test-tube-r")
                 put-p (.put p 0 0 10 (.length data) data)
                 watch-c (.watch c "test-tube-r")
                 reserve-c (.reserve-with-timeout c 10)]
             (println (str "test-reserve-with-timeout: result use => " use-p))
             (println (str "test-reserve-with-timeout: result put => " put-p))
             (println (str "test-reserve-with-timeout: result reserve => " reserve-c))
             (is (not (nil? reserve-c)))
             (is (> (:id reserve-c) 0))
             (is (= (:payload reserve-c) "hello-reserve-with-timeout"))
             (is (= true (.delete c (:id put-p)))))))


(deftest test-release
         (binding [ beanstalk.core/*debug* true ]
           (let [p (new-beanstalk) ; producer
                 c (new-beanstalk) ; consumer
                 use-p (.use p "test-tube")
                 put-p (.put p 0 0 10 5 "hello")
                 watch-c (.watch c "test-tube")
                 reserve-c (.reserve c)
                 release-c (.release c (:id reserve-c) 0 0)]
             (println (str "release: result use => " use-p))
             (println (str "release: result put => " put-p))
             (println (str "release: result reserve => " reserve-c))
             (is (not (nil? reserve-c)))
             (is (= true release-c))
             (is (> (:id reserve-c) 0))
             (is (= (:payload reserve-c) "hello"))
             (is (= true (.delete c (:id put-p)))))))

(deftest test-bury
         (binding [ beanstalk.core/*debug* true ]
           (let [p (new-beanstalk) ; producer
                 c (new-beanstalk) ; consumer
                 use-p (.use p "test-tube")
                 put-p (.put p 0 0 10 5 "hello")
                 watch-c (.watch c "test-tube")
                 reserve-c (.reserve c)
                 bury-c (.bury c (:id reserve-c) 1000)]
             (println (str "bury: result use => " use-p))
             (println (str "bury: result put => " put-p))
             (println (str "bury: result reserve => " reserve-c))
             (is (not (nil? reserve-c)))
             (is (= true bury-c))
             (is (> (:id reserve-c) 0))
             (is (= true (.delete c (:id put-p)))))))

(deftest test-touch
         (binding [ beanstalk.core/*debug* true ]
           (let [p (new-beanstalk) ; producer
                 c (new-beanstalk) ; consumer
                 use-p (.use p "test-tube")
                 put-p (.put p 0 0 10 5 "hello")
                 watch-c (.watch c "test-tube")
                 reserve-c (.reserve c)
                 touch-c (.touch c (:id reserve-c))]
             (println (str "touch: result use => " use-p))
             (println (str "touch: result put => " put-p))
             (println (str "touch: result reserve => " reserve-c))
             (is (not (nil? reserve-c)))
             (is (= true touch-c))
             (is (> (:id reserve-c) 0))
             (is (= true (.delete c (:id put-p)))))))

(deftest test-ignore
         (binding [ beanstalk.core/*debug* true ]
           (let [c (new-beanstalk) 
                 watch-c (.watch c "test-tube")
                 ignore-c (.ignore c "default")]
             (println (str "ignore: result ignore => " ignore-c))
             (is (> (:count ignore-c) 0)))))

(deftest test-peek
         (binding [ beanstalk.core/*debug* true ]
           (let [p (new-beanstalk) ; producer
                 c (new-beanstalk) ; consumer
                 data (str "hello " (rand-int 100))
                 use-p (.use p "test-tube-p")
                 put-p (.put p 0 0 10 (.length data) data)
                 watch-c (.watch c "test-tube-p")
                 reserve-c (.reserve c)
                 peek-c (.peek c (:id reserve-c))]
             (println (str "peek: result reserve => " reserve-c))
             (println (str "peek: result peek => " peek-c))
             (is (not (nil? reserve-c)))
             (is (not (nil? peek-c)))
             (is (= (:payload reserve-c) data))
             (is (= (:payload reserve-c) (:payload peek-c)))
             (is (= true (.delete c (:id peek-c)))))))

; test mismatch between length specified in put and the length of the data.
; should catch EXPECTED_CRLF
