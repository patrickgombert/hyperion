(ns hyperion.redis.meta-record-spec
  (:require [speclj.core :refer :all]
            [hyperion.redis.meta-record :refer :all]))

(describe "Redis Meta-Records"
  (context "defining db types"
    (it "handles strings"
      (should= "String" (db-type "some string")))

    (it "handles integers"
      (should= "Integer" (db-type 1)))

    (it "handles floats"
      (should= "Number" (db-type 1.1)))

    (it "handles booleans"
      (should= "Boolean" (db-type true)))

    (it "handles nils"
      (should= "Null" (db-type nil)))

    (it "handles vectors"
      (should= "Array" (db-type [])))

    (it "handles lists"
      (should= "Array" (db-type '()))
      (should= "Array" (db-type '(1))))

    (it "handles maps"
      (should= "Object" (db-type {})))

    (it "points all other types to Any"
      (should= "Any" (db-type (new java.util.HashMap)))
      (should= "Any" (db-type #()))
      (should= "Any" (db-type :any))))

  (context "casting from db types"
    (it "converts strings"
      (should= "some string" (from-db-type "some string" "String")))

    (it "converts integers"
      (should= 1 (from-db-type "1" "Integer")))

    (it "converts numbers"
      (should= 1.1 (from-db-type "1.1" "Number")))

    (it "converts booleans"
      (should= true (from-db-type "true" "Boolean")))

    (it "converts nulls"
      (should= nil (from-db-type "nil" "Null")))

    (it "converts arrays"
      (should= [] (from-db-type "[]" "Array"))
      (should= [1, 2.2, "three"] (from-db-type "[1,2.2,\"three\"]" "Array")))

    (it "converts objects"
      (should= {} (from-db-type "{}" "Object"))
      (should= {"one" 1 "two" 2.2 "three" "three"} (from-db-type "{\"one\":1,\"two\":2.2,\"three\":\"three\"}", "Object")))

    (it "converts any other types"
      (should= "any" (from-db-type "any" "Any"))))

  (it "builds a meta-record"
    (let [record {:string "str" :integer 1 :number 1.1 :boolean false
                  :null nil :array [] :object {}}]
      (should= {:string "String" :integer "Integer" :number "Number"
                :boolean "Boolean" :null "Null" :array "Array"
                :object "Object"} (meta-record record))))

  (it "constructs clojure data from a record and meta-record"
    (let [record {:string "str" :integer "1" :number "1.1" :boolean "false"
                  :null "nil" :array "[]" :object "{}"}
          meta-record {:string "String" :integer "Integer"
                :number "Number" :boolean "Boolean" :null "Null"
                :array "Array" :object "Object"}]
      (should= {:string "str" :integer 1 :number 1.1
                :boolean false :null nil :array [] :object {}}
        (cast-record record meta-record)))))
