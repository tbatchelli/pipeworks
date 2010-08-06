(ns pipeworks.core
  (:use [clojure.string :only (split)]
        [clojure.java.io :only (reader)])
  (:import (java.util StringTokenizer)
           (java.util.concurrent LinkedBlockingQueue)))

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

;;;; Autonomous pipeline stages

;;; each stage has one thread that reads from the in-queue, processes
;;; and queues the result on the out-queue
;;;
;;; Some logic is needed to programatically pipe the stages. Multiple
;;; pipe options should be provided (i.e. split and merge)
;;;
;;; consideration should be made to cleaning-up acquired resources when
;;; the computation is stopped, be it internally (exception,
;;; finalization) or externally (interruption)


(defprotocol stage
  (enqueue [this x])
  (dequeue [this]))

(defrecord single-thread-stage
  [queue]
  stage
  (enqueue [_ x]
           ;(println "queue: enqueue")
           (.put queue x))
  (dequeue [_]
           ;(println "queue: dequeue")
           (.take queue)))

(defn make-processor [process-fn in-queue out-queue]
  (let [process-one (fn []
                ;      (println "processor: processing one... ")
                      (let [element (.dequeue in-queue)]
                ;        (println "processor: dequeued... on to enqueueing")
                        (.enqueue out-queue (process-fn element))
                ;        (println "processor: enqueued... waiting")
                        ))
        process (fn []
               ;   (println "processor: starting stage... waiting")
                  (while true (process-one)))]
    (Thread. process)))

(defn bypass-worker [x] x)

(defrecord print-stage
  []
  stage
  (enqueue [_ x] (println "print stage: got" x))
  (dequeue [_] nil))


(comment
  (def in-queue (single-thread-stage. (LinkedBlockingQueue.)))
  (def out-queue (print-stage.))
  (def processor (make-processor reverse in-queue out-queue))
  (.start processor)
  (.enqueue in-queue "Hello world!"))