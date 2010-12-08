(ns beanstalk.core-test
  (:use beanstalk.core clojure.test))

(deftest test-beanstalk
         (let [b (Beanstalk-create)]
           (is (and (not (nil? b)) 
                    (instance? beanstalk.core.Beanstalk b)))))

(deftest test-stats
  (binding [ beanstalk.core/*debug* true ]
         (let [b (Beanstalk-create)
               result (.stats b)]
           (is (not (nil? result)))
           (is (> (.length result) 0)))))

(deftest test-put
  (binding [ beanstalk.core/*debug* true ]
         (let [b (Beanstalk-create)
               result (.put b 0 0 10 5 "hello")]
           (println (str "result => " result))
           (is (not (nil? result)))
           ; should be the job id
           (is (> (Integer. result) 0)))))

(deftest test-use
  (binding [ beanstalk.core/*debug* true ]
         (let [b (Beanstalk-create)
               result (.use b "test-tube")]
           (println (str "result => " result))
           (is (not (nil? result)))
           ; should be the job id
           (is (= result "test-tube")))))
