(ns pipeworks.core
  (:use [clojure.string :only (split)]
        [clojure.java.io :only (reader)])
  (:import (java.util StringTokenizer)))

(defn cat [url]
  (let [rdr (reader url)]
    (line-seq rdr)))

(defn tokenize [str delims]
  (let [tokenizer (StringTokenizer. str delims)]
    (enumeration-seq tokenizer)))

(defn select-columns [sq columns]
  (let [columnized (vec sq)]
    (map #(nth columnized %) columns))) 

(defn cut
  ([sq columns]
      (cut sq columns " "))
  ([sq columns delims]
      (if (seq sq)
        (lazy-seq (cons (tokenize (first sq) delims)
                        (cut (rest sq) delims)))
        nil)))

(defn grep [sq regex]
  (if (seq sq)
    (lazy-seq (if (re-find regex (first sq)) (cons (first sq) (grep (rest sq) regex))
                  (grep (rest sq) regex)))
    nil)) 

