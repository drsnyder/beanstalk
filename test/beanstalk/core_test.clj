(ns beanstalk.core-test
  (:use beanstalk.core clojure.test))

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
             (is (> (:id result) 0)))))

(deftest test-use
         (binding [ beanstalk.core/*debug* true ]
           (let [b (new-beanstalk)
                 result (.use b "test-tube")]
             (println (str "result => " result))
             (is (not (nil? result)))
             (is (= (:payload result) "test-tube")))))

(deftest test-reserve
         (binding [ beanstalk.core/*debug* true ]
           (let [b (new-beanstalk)
                 use-r (.use b "test-tube")
                 put-r (.put b 0 0 10 5 "hello")
                 reserve-r (.reserve b)]
             (println (str "result use => " use-r))
             (println (str "result put => " put-r))
             (println (str "result reserve => " reserve-r))
             (is (not (nil? reserve-r)))
             (is (= (:payload reserve-r) "hello")))))
