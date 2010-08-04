(ns pipeworks.test.core
  (:use [pipeworks.core] :reload-all)
  (:use [clojure.test]))

(deftest select-column
  (is (= '(a c e) (select-columns '(a b c d e f g) [0 2 4]))))