(ns kixi.hammer.specs
  (:require [clojure.core :exclude [integer? double?]]
            [clojure.spec.alpha :as spec]
            [java-time :as t]))

(defn str->int
  [^String s]
  (try
    (Integer/valueOf (clojure.string/replace s #"," "")) ;; also eliminate commas in 1000s
    (catch NumberFormatException e
      :clojure.spec/invalid)))

(defn -integer?
  [x]
  (cond (string? x) (str->int x)
        (clojure.core/integer? x) x
        :else :clojure.spec/invalid))

(def integer? (spec/conformer -integer?))

(defn str->double
  "Strings converted to doubles"
  [^String s]
  (try
    (Double/valueOf (str s))
    (catch Exception e
      :clojure.spec/invalid)))

(defn -double?
  [x]
  (cond
    (instance? Double x)  x
    (clojure.core/integer? x) (double x)
    (string? x) (str->double x)
    :else :clojure.spec/invalid))

(def double? (spec/conformer -double?))

(defn str->date
  [^String s]
  (try
    (t/local-date "dd/MM/yyyy" s)
    (catch Exception e
      :clojure.spec/invalid)))

(defn -date?
  [x]
  (cond
    (instance? java.time.LocalDate x) x
    (string? x) (str->date x)
    :else :clojure.spec/invalid))

(def date? (spec/conformer -date?))
