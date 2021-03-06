(ns hyperion.coerce
  (:require [chee.coerce :refer [AsInteger AsString AsBoolean AsKeyword]]))

; temporary holding place for coercion methods until they are moved into chee

(defprotocol AsByte
  (->byte [this]))

(defprotocol AsShort
  (->short [this]))

(defprotocol AsLong
  (->long [this]))

(defprotocol AsBigInteger
  (->biginteger [this]))

(defprotocol AsFloat
  (->float [this]))

(defprotocol AsDouble
  (->double [this]))

(defprotocol AsCharacter
  (->char [this]))

(extend-protocol AsBoolean
  java.lang.Integer
  (->bool [this] (= this 1))

  java.lang.Long
  (->bool [this] (= this 1))

  nil
  (->bool [this] this))

(extend-protocol AsByte
  java.lang.Byte
  (->byte [this] this)

  java.lang.Integer
  (->byte [this] (.byteValue this))

  java.lang.Long
  (->byte [this] (.byteValue this))

  java.lang.String
  (->byte [this] (Byte. this))

  nil
  (->byte [this] nil)

  )

(extend-protocol AsInteger

  nil
  (->int [this] nil)

  )

(extend-protocol AsLong
  java.lang.Integer
  (->long [this] (.longValue this))

  java.lang.Long
  (->long [this] this)

  java.lang.String
  (->long [this] (long (BigInteger. this)))

  nil
  (->long [this] nil)

  )

(extend-protocol AsFloat
  java.lang.String
  (->float [this] (Float. this))

  java.lang.Float
  (->float [this] this)

  java.lang.Long
  (->float [this] (.floatValue this))

  java.lang.Integer
  (->float [this] (.floatValue this))

  java.lang.Double
  (->float [this] (.floatValue this))

  nil
  (->float [this] nil)

  )

(extend-protocol AsDouble
  java.lang.String
  (->double [this] (Double. this))

  java.lang.Float
  (->double [this] (.doubleValue this))

  java.lang.Double
  (->double [this] this)

  nil
  (->double [this] nil)

  )

(extend-protocol AsString

  Object
  (->string [this] (str this))

  nil
  (->string [this] nil)

  java.lang.Character
  (->string [this] (String/valueOf this))

  )

(extend-protocol AsKeyword

  nil
  (->keyword [this] nil)

  )

(extend-protocol AsShort
  java.lang.Number
  (->short [this] (.shortValue this))

  nil
  (->short [this] nil)

  java.lang.String
  (->short [this] (Short. this))

  )

(extend-protocol AsCharacter

  java.lang.Character
  (->char [this] this)

  java.lang.String
  (->char [this] (.charAt this 0))

  nil
  (->char [this] nil)

  )
