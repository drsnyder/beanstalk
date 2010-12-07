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

