(ns beanstalk.core-test
  (:use beanstalk.core clojure.test))

(deftest test-beanstalk
  (let [b (beanstalk)]
    (is (and (not (nil? b)) 
             (instance? clojure.lang.PersistentStructMap b)))))

(deftest test-beanstalk-write
  (let [b (beanstalk)]
    (is (not (nil? (beanstalk-write b "stats\r\n"))))))

(deftest test-beanstalk-read
   (is (= "OK" (re-find #"^OK" 
                        (beanstalk-read 
                          (beanstalk-write 
                            (beanstalk) "stats\r\n"))))))
