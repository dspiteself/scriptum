(ns scriptum.nested-test
  "Pins Elasticsearch-style nested documents: Lucene block joins, children
  first and their root last, and nested objects inside nested objects, each
  right after its own children.

  Block joins need nothing from scriptum's storage — fork, forceMerge,
  merge-from! and the konserve paths all carry a block through unchanged. What
  they need is that NO WRITE EVER SPLITS ONE. Delete a root and leave its
  children live, and a nested query at once returns the DELETED root through
  them; after the next merge the orphans silently belong to the following root,
  and a nested query answers for a document that never had them. CheckJoinIndex
  passes again by then, so it is run after every mutation here, while the split
  is still visible: a deleted root with a live child is exactly what it
  reports. Alongside it runs the same check for every inner level, which
  CheckJoinIndex cannot make (see `segment-structure`)."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [konserve.store :as kstore]
            [scriptum.core :as sc])
  (:import [clojure.lang ExceptionInfo]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]
           [java.time Instant]
           [org.apache.lucene.document Document Field$Store IntField LongField
            StringField]
           [org.apache.lucene.index IndexReader LeafReader LeafReaderContext
            PostingsEnum Term Terms TermsEnum]
           [org.apache.lucene.search BoostQuery ConstantScoreQuery DisjunctionMaxQuery
            DocIdSetIterator IndexOrDocValuesQuery IndexSearcher MatchAllDocsQuery
            Query TermQuery]
           [org.apache.lucene.search.join CheckJoinIndex ScoreMode ToChildBlockJoinQuery
            ToParentBlockJoinQuery]
           [org.apache.lucene.util Bits BytesRef]
           [org.replikativ.scriptum BranchIndexWriter NestedQuery]))

(def ^:dynamic *root* nil)

(defn- rm-rf [^java.io.File f]
  (when (.isDirectory f) (run! rm-rf (.listFiles f)))
  (.delete f))

(use-fixtures :each
  (fn [t]
    (let [root (str (Files/createTempDirectory "scriptum-nested-test-"
                                               (make-array FileAttribute 0)))]
      (try (binding [*root* root] (t))
           (finally (rm-rf (io/file root)))))))

(defn- under [name] (str *root* "/" name))

(defn- store-at
  "A fresh konserve store with an identity.

  `create-store` with a UUID `:id`, never `connect-fs-store`: a store that
  answers nil for its id silently disables scriptum's GC guard. Each test opens
  its store exactly once, so minting the id here cannot give one store two."
  [path]
  (kstore/create-store {:backend :file :path path :id (random-uuid)}
                       {:sync? true}))

;; --- Block integrity ---

(defn- segment-paths
  "Each document's nested path in `leaf`, in doc-id order; nil for a root.

  Read from the terms index, since `_nested_path` is not stored. Postings keep
  a deleted document until a merge drops it, so deleted documents have their
  paths too, and a child's liveness can be compared with its parent's."
  [^LeafReader leaf]
  (let [paths (object-array (.maxDoc leaf))]
    (when-let [^Terms terms (.terms leaf sc/nested-path-field)]
      (let [^TermsEnum terms-enum (.iterator terms)]
        (loop []
          (when-let [^BytesRef term (.next terms-enum)]
            (let [path (.utf8ToString term)
                  ^PostingsEnum postings (.postings terms-enum nil (int PostingsEnum/NONE))]
              (loop []
                (let [doc (.nextDoc postings)]
                  (when (not= doc DocIdSetIterator/NO_MORE_DOCS)
                    (aset paths doc path)
                    (recur)))))
            (recur)))))
    (vec paths)))

(defn- path-line
  "The paths above `path`, nearest first, ending in nil for the root."
  [^String path]
  (let [i (.lastIndexOf path ".")]
    (if (pos? i)
      (let [up (subs path 0 i)]
        (cons up (path-line up)))
      [nil])))

(defn- segment-structure
  "nil when, in one segment's `paths`, every child is followed by its parent
  before any other of its ancestors' documents and is live exactly when that
  parent is; otherwise what is wrong.

  CheckJoinIndex for every level at once. CheckJoinIndex itself takes one
  parents filter for the whole index, and only the roots' fits every segment:
  with the documents on \"comments\" as the parents, a segment ending in a
  root fails it. A block join over the documents on any path P takes, for a
  document below P, the first P document after it, so this order is exactly
  what makes every level's join find the right ancestor."
  [paths live?]
  (let [n (count paths)]
    (cond
      (zero? n) nil
      (some? (peek paths)) (str "the last document, " (dec n) ", is a child on " (peek paths))
      :else
      (loop [i (dec n)
             next-at {}]
        (when-not (neg? i)
          (let [path (nth paths i)]
            (if (nil? path)
              (recur (dec i) (assoc next-at nil i))
              (let [[parent & above] (path-line path)
                    at (get next-at parent)
                    sooner (when at
                             (some #(when-let [k (get next-at %)] (when (< k at) [% k]))
                                   above))]
                (cond
                  (nil? at)
                  (format "child %d on %s has no parent on %s after it"
                          i path (or parent "the roots"))

                  sooner
                  (format "child %d on %s meets %s at %d before its parent at %d"
                          i path (or (first sooner) "a root") (second sooner) at)

                  (not= (live? i) (live? at))
                  (format "child %d on %s is %s but its parent %d is not"
                          i path (if (live? i) "live" "deleted") at)

                  :else
                  (recur (dec i) (assoc next-at path i)))))))))))

(defn- block-integrity
  "`:intact`, or a description of the first split or misordered block: by
  CheckJoinIndex over the roots, then by `segment-structure` at every level."
  [^IndexReader reader]
  (try (CheckJoinIndex/check reader sc/roots-bitset)
       (or (some (fn [^LeafReaderContext ctx]
                   (let [leaf (.reader ctx)
                         ^Bits live (.getLiveDocs leaf)]
                     (segment-structure (segment-paths leaf)
                                        #(or (nil? live) (.get live (int %))))))
                 (.leaves reader))
           :intact)
       (catch IllegalStateException e (.getMessage e))))

(defn- assert-blocks-intact [reader]
  (is (= :intact (block-integrity reader))))

(defn- assert-writer-blocks-intact
  "Check the blocks a fresh NRT reader over `w` sees.

  Opening that reader FLUSHES the writer's buffer, so a test about buffered
  writes must not call this until the buffered state has been acted on."
  [w]
  (with-open [r (sc/snapshot w)]
    (assert-blocks-intact r)))

(defn- latched?
  "Whether `w` takes the block-safe delete path: BranchIndexWriter's
  may-hold-nested latch."
  [w]
  (.mayHoldNested ^BranchIndexWriter (sc/->writer w)))

;; --- Documents ---

(defn- comment-spec [[author stars]]
  {:author {:value author :type :string}
   :stars {:value stars :type :int}})

(defn- post
  "A root carrying a root-only `title`, with one :comments child per
  [author stars]."
  [id title comments]
  {:id {:value id :type :string}
   :title {:value title :type :string}
   :comments {:type :nested :value (mapv comment-spec comments)}})

(def ^:private seed-posts
  "p1 has an alice comment and a 1-star comment, but not one that is both —
  the case a flattened mapping gets wrong."
  [["p1" "one" [["alice" 5] ["bob" 1]]]
   ["p2" "two" [["carol" 3]]]
   ["p3" "three" [["alice" 1]]]])

(def ^:private seed-doc-count
  "Lucene documents in `seed-posts`: 3 roots and 4 children."
  7)

(defn- seed! [w posts]
  (doseq [[id title comments] posts]
    (sc/add-doc w (post id title comments))))

(defn- prebuilt-child
  "A :comments child built by hand, as an `add-block` caller would."
  ^Document [^String author]
  (doto (Document.)
    (.add (StringField. sc/nested-path-field "comments" Field$Store/NO))
    (.add (StringField. "comments.author" author Field$Store/YES))))

(defn- prebuilt-root ^Document [^String id]
  (doto (Document.)
    (.add (StringField. "id" id Field$Store/YES))))

;; --- Queries ---

(defn- comment-q
  "Child-level conjunction: ONE comment by `author` with exactly `stars`."
  [author stars]
  (sc/bool-query [[{:term [:comments.author author]} :filter]
                  [(IntField/newExactQuery "comments.stars" (int stars)) :filter]]))

(defn- ids
  "Result ids, sorted but not deduplicated, so a repeated root shows."
  [results]
  (vec (sort (map #(get % "id") results))))

(defn- search-ids [w query]
  (ids (sc/search w query {:limit 100})))

(defn- nested-ids
  "The roots with a comment by `author` with exactly `stars`."
  [w author stars]
  (search-ids w (sc/nested-query "comments" (comment-q author stars))))

;; =============================================================================
;; Query semantics
;; =============================================================================

(deftest nested-query-matches-within-one-child
  (testing "ES `nested`: every condition must hold in the SAME child, which is
            the whole difference from a flattened object mapping"
    (let [w (sc/create-index (under "nested") "main")
          flat (sc/create-index (under "flat") "main")]
      (try
        (is (= "_nested_path" sc/nested-path-field))
        (seed! w seed-posts)
        (sc/commit! w)
        (assert-writer-blocks-intact w)
        (is (= ["p3"] (nested-ids w "alice" 1))
            "p1 has alice and a 1-star comment, but in different comments")
        (is (= ["p1"] (nested-ids w "alice" 5)))
        (is (= [] (nested-ids w "carol" 1)))
        (is (= ["p1" "p3"]
               (search-ids w (sc/nested-query
                              "comments" {:term [:comments.author "alice"]}))))
        (is (= [] (search-ids w (comment-q "alice" 1)))
            "child fields live on children, so a plain query over them finds no root")

        ;; What an ES `object` mapping indexes for p1: every comment's fields
        ;; flattened onto the one document.
        (sc/add-doc flat {:id {:value "p1" :type :string}
                          :comments.author {:value ["alice" "bob"] :type :string}
                          :comments.stars {:value [5 1] :type :int}})
        (sc/commit! flat)
        (is (= ["p1"] (search-ids flat (comment-q "alice" 1)))
            "flattened, the conjunction matches ACROSS comments — the false
             positive nested exists to remove")
        (finally
          (sc/close! w)
          (sc/close! flat))))))

(deftest nested-query-score-modes
  (let [w (sc/create-index (under "idx") "main")]
    (try
      (seed! w seed-posts)
      (sc/commit! w)
      (assert-writer-blocks-intact w)
      ;; bob is rarer than alice, so a bob child outscores an alice child and
      ;; p1's two matching children have distinct scores to aggregate.
      (let [either (sc/bool-query [[{:term [:comments.author "alice"]} :should]
                                   [{:term [:comments.author "bob"]} :should]])
            scores (fn [q]
                     (into {} (map (juxt #(get % "id") :score))
                           (sc/search w q {:limit 10})))
            by-mode (into {}
                          (map (fn [mode]
                                 [mode (scores (sc/nested-query
                                                "comments" either
                                                {:score-mode mode}))]))
                          [:avg :max :min :sum :none])
            p1 #(get-in by-mode [% "p1"])]
        (testing "every mode runs and matches the same roots"
          (doseq [[mode s] by-mode]
            (is (= #{"p1" "p3"} (set (keys s))) (str mode))))
        (testing "the modes aggregate p1's two child scores as ES names them"
          (is (> (p1 :sum) (p1 :max) (p1 :avg) (p1 :min) 0)))
        (testing "one matching child scores the same under every aggregating mode"
          (is (apply = (map #(get-in by-mode [% "p3"]) [:avg :max :min :sum]))))
        (testing ":none computes no child score: every root scores 0, as in ES"
          (is (every? zero? (vals (by-mode :none)))))
        (testing ":avg is the default, as in ES"
          (is (= (by-mode :avg) (scores (sc/nested-query "comments" either))))))
      (testing "the child query may be the {:term [field value]} form"
        (is (= ["p2"] (search-ids w (sc/nested-query
                                     "comments" {:term [:comments.author "carol"]}
                                     {:score-mode :max})))))
      (testing "an unknown score mode is refused rather than defaulted"
        (is (thrown? ExceptionInfo
                     (sc/nested-query "comments" {:term [:comments.author "carol"]}
                                      {:score-mode :median}))))
      (finally (sc/close! w)))))

(deftest children-are-hidden-from-ordinary-queries
  (testing "a child is not a document to the caller: every ordinary query path
            answers in roots, and :all counts what ES's _count counts"
    (let [s (store-at (under "store"))
          c (under "cache")
          w (sc/open-store-index s c "main")]
      (try
        (seed! w seed-posts)
        (sc/commit! w)
        (assert-writer-blocks-intact w)
        (testing "a live search"
          (is (= ["p1" "p2" "p3"] (search-ids w :all)))
          (is (= [] (search-ids w "alice"))
              "a string query over a child-only value returns no child")
          (is (= [] (search-ids w {:term [:comments.author "carol"]}))))
        (testing "num-docs counts Lucene documents, children included — ES's
                  index-stats docs.count, not its _count"
          (is (= seed-doc-count (sc/num-docs w))))
        (with-open [snap (sc/open-store-snapshot s c (sc/snapshot-address w))]
          (assert-blocks-intact (:reader snap))
          (is (= 3 (sc/count-store-snapshot snap :all)))
          (is (= 0 (sc/count-store-snapshot snap "alice")))
          (is (= 2 (sc/count-store-snapshot
                    snap (sc/nested-query "comments"
                                          {:term [:comments.author "alice"]}))))
          (is (= ["p1" "p2" "p3"]
                 (ids (sc/search-store-snapshot snap :all {:limit 100}))))
          (let [{:keys [candidates exhausted?]}
                (sc/candidate-page snap :all {:page-size 100})]
            (is exhausted?)
            (is (= ["p1" "p2" "p3"] (ids candidates)))))
        (finally (sc/close! w))))))

(deftest indexes-without-nested-documents-are-untouched
  (testing "no nested document, no root filter: the query a snapshot runs is
            the caller's own object, so nothing about an existing index moves"
    (let [s (store-at (under "store"))
          c (under "cache")
          w (sc/open-store-index s c "main")]
      (try
        (doseq [i (range 5)]
          (sc/add-doc w {:id {:value (str "d" i) :type :string}
                         :body "same text"}))
        (sc/commit! w)
        (with-open [snap (sc/open-store-snapshot s c (sc/snapshot-address w))]
          (assert-blocks-intact (:reader snap))
          (let [q (sc/text-query :body "same")]
            (is (identical? q (get-in (sc/candidate-page snap q {:page-size 2})
                                      [:continuation :query]))))
          (is (= (TermQuery. (Term. "body" "same"))
                 (get-in (sc/candidate-page snap {:term [:body "same"]}
                                            {:page-size 2})
                         [:continuation :query])))
          (is (= (MatchAllDocsQuery.)
                 (get-in (sc/candidate-page snap :all {:page-size 2})
                         [:continuation :query])))
          (is (= 5 (sc/count-store-snapshot snap :all))))
        (finally (sc/close! w)))))
  (testing "update-doc, delete-docs and delete-query keep their term semantics"
    (let [w (sc/create-index (under "disk") "main")]
      (try
        (doseq [[id body] [["a" "alpha"] ["b" "beta"] ["c" "gamma"] ["d" "delta"]]]
          (sc/add-doc w {:id {:value id :type :string} :body body}))
        (sc/commit! w)
        (is (not (latched? w)))
        (sc/update-doc w "id" "a" {:id {:value "a" :type :string}
                                   :body "revised"})
        (sc/delete-docs w "id" "b")
        (sc/delete-query w (TermQuery. (Term. "id" "c")))
        (sc/commit! w)
        ;; On a flat index both paths give these results; only the latch says
        ;; which one ran, and a flat index must stay on the term path.
        (is (not (latched? w)) "no flat write moves a flat index to the query path")
        (assert-writer-blocks-intact w)
        (is (= 2 (sc/num-docs w)))
        (is (= ["a" "d"] (search-ids w :all)))
        (is (= ["a"] (search-ids w {:term [:body "revised"]})))
        (is (= [] (search-ids w {:term [:body "alpha"]}))
            "the update replaced the old document rather than adding beside it")
        (finally (sc/close! w))))))

(deftest child-maps-take-the-top-level-field-spec
  (let [w (sc/create-index (under "idx") "main")]
    (try
      (sc/add-doc w {:id {:value "p" :type :string}
                     :comments {:type :nested
                                :value [{:body "Great first post"
                                         :tags {:value ["x" "y"] :type :string}
                                         :at (Instant/ofEpochMilli 1000)
                                         :stars {:value 4 :type :int :store? false}}]}})
      (sc/add-doc w {:id {:value "q" :type :string}
                     :comments {:type :nested :value []}})
      (sc/commit! w)
      (assert-writer-blocks-intact w)
      (let [hit #(search-ids w (sc/nested-query "comments" %))]
        (is (= ["p"] (hit (sc/text-query "comments.body" "great")))
            "an untyped string is analyzed text, under its prefixed name")
        (is (= ["p"] (hit {:term [:comments.tags "y"]}))
            "every value of a multi-valued field is indexed")
        (is (= ["p"] (hit (LongField/newExactQuery "comments.at" 1000)))
            "an Instant auto-detects to :long")
        (is (= ["p"] (hit (IntField/newExactQuery "comments.stars" 4))))
        (is (= [] (hit {:term [:tags "y"]}))
            "a child field is named for its path, as in ES"))
      (testing "an empty :nested value writes a root with no children"
        (is (= ["p" "q"] (search-ids w :all)))
        (is (= 3 (sc/num-docs w))))
      (finally (sc/close! w)))))

(deftest each-nested-field-is-its-own-path
  (let [w (sc/create-index (under "idx") "main")]
    (try
      (sc/add-doc w {:id {:value "r" :type :string}
                     :comments {:type :nested
                                :value [{:author {:value "alice" :type :string}}]}
                     :reviews {:type :nested
                               :value [{:author {:value "bob" :type :string}}
                                       {:author {:value "alice" :type :string}}]}})
      (sc/add-doc w {:id {:value "s" :type :string}
                     :reviews {:type :nested
                               :value [{:author {:value "carol" :type :string}}]}})
      (sc/commit! w)
      (assert-writer-blocks-intact w)
      (is (= 6 (sc/num-docs w)) "both paths' children precede the one root")
      (is (= ["r"] (search-ids w (sc/nested-query
                                  "comments" {:term [:comments.author "alice"]}))))
      (is (= ["r"] (search-ids w (sc/nested-query
                                  "reviews" {:term [:reviews.author "bob"]}))))
      (is (= ["s"] (search-ids w (sc/nested-query
                                  "reviews" {:term [:reviews.author "carol"]}))))
      (is (= [] (search-ids w (sc/nested-query
                               "comments" {:term [:reviews.author "bob"]})))
          "the path filter keeps one path's query off another path's children")
      (sc/delete-docs w "id" "r")
      (sc/commit! w)
      (assert-writer-blocks-intact w)
      (is (= 2 (sc/num-docs w)) "a root takes the children of every path with it")
      (is (= ["s"] (search-ids w :all)))
      (finally (sc/close! w)))))

;; =============================================================================
;; Storage paths
;; =============================================================================

(deftest a-fork-rewrites-blocks-without-touching-its-parent
  (let [main (sc/create-index (under "idx") "main")]
    (try
      (seed! main seed-posts)
      (sc/commit! main)
      (assert-writer-blocks-intact main)
      (let [exp (sc/fork main "exp")]
        (try
          (sc/update-doc exp "id" "p1" (post "p1" "one" [["dave" 2]]))
          (assert-writer-blocks-intact exp)
          (sc/update-doc exp "id" "p2" (post "p2" "two" [["carol" 3] ["erin" 4]
                                                         ["frank" 5]]))
          (assert-writer-blocks-intact exp)
          (sc/commit! exp)
          (testing "the branch sees its new blocks"
            (is (= ["p1"] (nested-ids exp "dave" 2)))
            (is (= [] (nested-ids exp "alice" 5))
                "fewer children: the dropped ones went with the old block")
            (is (= [] (nested-ids exp "bob" 1)))
            (is (= ["p2"] (nested-ids exp "carol" 3)))
            (is (= ["p2"] (nested-ids exp "frank" 5)))
            (is (= ["p1" "p2" "p3"] (search-ids exp :all)))
            (is (= 8 (sc/num-docs exp)) "3 roots and 1 + 3 + 1 children, no leftovers"))
          (testing "the parent is unchanged"
            (assert-writer-blocks-intact main)
            (is (= ["p1"] (nested-ids main "alice" 5)))
            (is (= [] (nested-ids main "dave" 2)))
            (is (= [] (nested-ids main "frank" 5)))
            (is (= seed-doc-count (sc/num-docs main))))
          (finally (sc/close! exp))))
      (finally (sc/close! main)))))

(deftest force-merge-keeps-blocks-whole
  (let [w (sc/create-index (under "idx") "main")]
    (try
      ;; A commit per block, then an update and a delete, so the merge has
      ;; several segments and deletions to reconcile.
      (doseq [p seed-posts]
        (seed! w [p])
        (sc/commit! w)
        (assert-writer-blocks-intact w))
      (sc/add-doc w (post "p4" "four" [["gus" 2] ["hal" 3]]))
      (sc/commit! w)
      (sc/update-doc w "id" "p2" (post "p2" "two" [["carol" 4]]))
      (sc/delete-docs w "id" "p3")
      (sc/commit! w)
      (with-open [r (sc/snapshot w)]
        (is (< 1 (count (.leaves r))) "precondition: there is something to merge")
        (assert-blocks-intact r))
      (.forceMerge (sc/->writer w) 1)
      (sc/commit! w)
      (with-open [r (sc/snapshot w)]
        (is (= 1 (count (.leaves r))))
        (assert-blocks-intact r))
      (is (= ["p1"] (nested-ids w "alice" 5)))
      (is (= [] (nested-ids w "alice" 1)) "p3's child was deleted with it")
      (is (= ["p2"] (nested-ids w "carol" 4)))
      (is (= [] (nested-ids w "carol" 3)))
      (is (= ["p4"] (nested-ids w "hal" 3)))
      (is (= ["p1" "p2" "p4"] (search-ids w :all)))
      (is (= 8 (sc/num-docs w)) "p1 1+2, p2 1+1, p4 1+2")
      (finally (sc/close! w)))))

(deftest merge-from-keeps-blocks-whole
  (testing "merge-from! is add-only, so shared blocks arrive twice — what must
            hold is that each arrives whole"
    (let [main (sc/create-index (under "idx") "main")]
      (try
        (seed! main (take 2 seed-posts))
        (sc/commit! main)
        (let [exp (sc/fork main "exp")]
          (try
            (sc/update-doc exp "id" "p1" (post "p1" "one" [["erin" 2]]))
            (sc/add-doc exp (post "p4" "four" [["frank" 1]]))
            (sc/commit! exp)
            (assert-writer-blocks-intact exp)
            (sc/merge-from! main exp)
            (assert-writer-blocks-intact main)
            (is (= #{"p1"} (set (nested-ids main "erin" 2))))
            (is (= #{"p4"} (set (nested-ids main "frank" 1))))
            (is (= #{"p2"} (set (nested-ids main "carol" 3))))
            (is (= 11 (sc/num-docs main))
                "main's 5 documents plus exp's 6 live ones; exp's replaced p1
                 block stays behind whole, children included")
            (.forceMerge (sc/->writer main) 1)
            (sc/commit! main)
            (assert-writer-blocks-intact main)
            (is (= #{"p4"} (set (nested-ids main "frank" 1))))
            (is (= #{"p2"} (set (nested-ids main "carol" 3))))
            (finally (sc/close! exp))))
        (finally (sc/close! main))))))

;; =============================================================================
;; Writes treat a block as one unit
;; =============================================================================

(deftest deleting-a-root-takes-its-children
  (testing "REGRESSION, silent corruption: a delete that matched only the root
            left its children live, and after a merge they belonged to the NEXT
            root — carol's comment turned up under p3, and CheckJoinIndex
            passed. A delete matches roots only and takes each one's block."
    (doseq [[label delete!] [["delete-docs"
                              #(sc/delete-docs % "title" "two")]
                             ["delete-query"
                              #(sc/delete-query % (TermQuery. (Term. "title" "two")))]
                             ;; ES's delete_by_query with a nested clause: the
                             ;; query names a child, and its ROOT is what goes.
                             ["delete-query by a nested-query"
                              #(sc/delete-query % (sc/nested-query
                                                   "comments"
                                                   {:term [:comments.author "carol"]}))]]]
      (testing label
        (let [w (sc/create-index (under label) "main")]
          (try
            (seed! w seed-posts)
            (sc/commit! w)
            (delete! w)
            (sc/commit! w)
            (assert-writer-blocks-intact w)
            (is (= [] (nested-ids w "carol" 3)))
            (is (= ["p1" "p3"] (search-ids w :all)))
            (is (= 5 (sc/num-docs w)) "p2 and its child, and nothing else")
            (.forceMerge (sc/->writer w) 1)
            (sc/commit! w)
            (assert-writer-blocks-intact w)
            (is (= [] (nested-ids w "carol" 3))
                "p2's child must not re-parent onto p3")
            (is (= ["p3"] (nested-ids w "alice" 1)))
            (is (= ["p1"] (nested-ids w "bob" 1)))
            (is (= 5 (sc/num-docs w)))
            (finally (sc/close! w)))))))
  (testing "a child is not deletable on its own, as in ES: reindex its root"
    (doseq [[label delete!] [["delete-docs"
                              #(sc/delete-docs % "comments.author" "carol")]
                             ["delete-query"
                              #(sc/delete-query
                                % (TermQuery. (Term. "comments.author" "carol")))]]]
      (testing label
        (let [w (sc/create-index (under (str "child-" label)) "main")]
          (try
            (seed! w seed-posts)
            (sc/commit! w)
            (delete! w)
            (sc/commit! w)
            (assert-writer-blocks-intact w)
            (is (= seed-doc-count (sc/num-docs w)) "nothing was deleted")
            (is (= ["p2"] (nested-ids w "carol" 3)))
            (finally (sc/close! w)))))))
  (testing "nor replaceable: update-doc keyed by a child-only value matches no
            root, so the old block stays and the new one is added beside it"
    (let [w (sc/create-index (under "child-update") "main")]
      (try
        (seed! w seed-posts)
        (sc/commit! w)
        (sc/update-doc w "comments.author" "carol" (post "p2" "two" [["carol" 9]]))
        (sc/commit! w)
        (assert-writer-blocks-intact w)
        (is (= ["p1" "p2" "p2" "p3"] (search-ids w :all)))
        (is (= ["p2"] (nested-ids w "carol" 3)) "the old block is untouched")
        (is (= ["p2"] (nested-ids w "carol" 9)))
        (is (= (+ seed-doc-count 2) (sc/num-docs w)))
        (finally (sc/close! w)))))
  (testing "a query matching roots and children alike deletes every block"
    (let [w (sc/create-index (under "match-all") "main")]
      (try
        (seed! w seed-posts)
        (sc/commit! w)
        (sc/delete-query w (MatchAllDocsQuery.))
        (sc/commit! w)
        (assert-writer-blocks-intact w)
        (is (= 0 (sc/num-docs w)))
        (finally (sc/close! w))))))

(deftest update-doc-moves-a-root-between-nested-and-flat
  (testing "on an index with nested documents"
    (let [w (sc/create-index (under "nested") "main")]
      (try
        (seed! w seed-posts)
        (sc/commit! w)
        (sc/update-doc w "id" "p1" {:id {:value "p1" :type :string}
                                    :title {:value "one, flat" :type :string}})
        (sc/commit! w)
        (assert-writer-blocks-intact w)
        (is (= [] (nested-ids w "alice" 5)) "the old children went with the old root")
        (is (= [] (nested-ids w "bob" 1)))
        (is (= ["p1" "p2" "p3"] (search-ids w :all)))
        (is (= ["p1"] (search-ids w {:term [:title "one, flat"]})))
        (is (= 5 (sc/num-docs w)))
        (sc/update-doc w "id" "p1" (post "p1" "one" [["alice" 5] ["bob" 1]]))
        (sc/commit! w)
        (assert-writer-blocks-intact w)
        (is (= ["p1"] (nested-ids w "alice" 5)))
        (is (= [] (search-ids w {:term [:title "one, flat"]})))
        (is (= seed-doc-count (sc/num-docs w)))
        (.forceMerge (sc/->writer w) 1)
        (sc/commit! w)
        (assert-writer-blocks-intact w)
        (is (= ["p1"] (nested-ids w "alice" 5)))
        (is (= ["p2"] (nested-ids w "carol" 3)) "nothing re-parented in the merge")
        (is (= ["p3"] (nested-ids w "alice" 1)))
        (is (= seed-doc-count (sc/num-docs w)))
        (finally (sc/close! w)))))
  (testing "on an index that has never held a nested document"
    (let [w (sc/create-index (under "flat") "main")]
      (try
        (sc/add-doc w {:id {:value "q1" :type :string} :body "plain"})
        (sc/add-doc w {:id {:value "q2" :type :string} :body "plain"})
        (sc/commit! w)
        (sc/update-doc w "id" "q1" (post "q1" "first" [["alice" 5] ["bob" 1]]))
        (sc/commit! w)
        (assert-writer-blocks-intact w)
        (is (= ["q1"] (nested-ids w "alice" 5)))
        (is (= ["q1" "q2"] (search-ids w :all)))
        (is (= 4 (sc/num-docs w)))
        (sc/update-doc w "id" "q1" {:id {:value "q1" :type :string} :body "plain again"})
        (sc/commit! w)
        (assert-writer-blocks-intact w)
        (is (= [] (nested-ids w "alice" 5)))
        (is (= ["q1" "q2"] (search-ids w :all)))
        (is (= 2 (sc/num-docs w)) "the children left with the block they were in")
        (finally (sc/close! w))))))

(deftest buffered-nested-writes-are-seen-by-block-deletes
  (testing "the nested check has to see documents still in the RAM buffer: a
            fresh index whose only nested blocks are unflushed must not take
            the plain term path"
    (let [w (sc/create-index (under "delete") "main")]
      (try
        ;; No reader, flush or commit between these writes — any of them would
        ;; flush, and the point is the state before one.
        (seed! w (take 2 seed-posts))
        (sc/delete-docs w "id" "p1")
        (sc/commit! w)
        (assert-writer-blocks-intact w)
        (is (= [] (nested-ids w "alice" 5)))
        (is (= ["p2"] (search-ids w :all)))
        (is (= 2 (sc/num-docs w)) "p2 and its child")
        (.forceMerge (sc/->writer w) 1)
        (sc/commit! w)
        (assert-writer-blocks-intact w)
        (is (= [] (nested-ids w "alice" 5))
            "p1's children must not re-parent onto p2")
        (is (= [] (nested-ids w "bob" 1)))
        (is (= ["p2"] (nested-ids w "carol" 3)))
        (finally (sc/close! w)))))
  (testing "and update-doc replaces a buffered block whole"
    (let [w (sc/create-index (under "update") "main")]
      (try
        (seed! w (take 1 seed-posts))
        (sc/update-doc w "id" "p1" (post "p1" "one" [["zed" 1]]))
        (sc/commit! w)
        (assert-writer-blocks-intact w)
        (is (= ["p1"] (nested-ids w "zed" 1)))
        (is (= [] (nested-ids w "alice" 5)))
        (is (= 2 (sc/num-docs w)))
        (finally (sc/close! w))))))

(deftest every-way-children-arrive-sets-the-latch
  (testing "the writes that bring a branch its first children"
    (doseq [[label write!] [["add-doc" #(sc/add-doc % (post "p1" "one" [["alice" 5]]))]
                            ["add-block" #(sc/add-block % [(prebuilt-child "alice")
                                                           (prebuilt-root "p1")])]
                            ["update-doc" #(sc/update-doc % "id" "p1"
                                                          (post "p1" "one" [["alice" 5]]))]]]
      (testing label
        (let [w (sc/create-index (under label) "main")]
          (try
            (sc/add-doc w {:id {:value "flat" :type :string}})
            (is (not (latched? w)))
            (write! w)
            (is (latched? w))
            (finally (sc/close! w)))))))
  (testing "a branch opened on a commit holding children starts latched, and a
            flat one does not"
    (let [nested (under "reopen-nested")
          flat (under "reopen-flat")]
      (let [w (sc/create-index nested "main")]
        (seed! w (take 2 seed-posts))
        (sc/commit! w)
        (sc/close! w))
      (let [w (sc/create-index flat "main")]
        (sc/add-doc w {:id {:value "flat" :type :string}})
        (sc/commit! w)
        (sc/close! w))
      (let [w (sc/open-branch nested "main")]
        (try
          (is (latched? w))
          (sc/delete-docs w "id" "p1")
          (sc/commit! w)
          (assert-writer-blocks-intact w)
          (is (= 2 (sc/num-docs w)) "p1 went with both its children")
          (let [exp (sc/fork w "exp")]
            (try (is (latched? exp) "and so does a fork of it")
                 (finally (sc/close! exp))))
          (finally (sc/close! w))))
      (let [w (sc/open-branch flat "main")]
        (try (is (not (latched? w)))
             (finally (sc/close! w))))))
  (testing "merge-from! of a nested fork into a main that never held a child.
            addIndexes brings the blocks without passing through addDocuments,
            so unless the merge itself sets the latch, main deletes the merged
            roots by term and their children re-parent"
    (let [main (sc/create-index (under "merge") "main")]
      (try
        (sc/add-doc main {:id {:value "m1" :type :string}})
        (sc/commit! main)
        (let [exp (sc/fork main "exp")]
          (try
            (sc/add-doc exp (post "p1" "one" [["alice" 5] ["bob" 1]]))
            (sc/commit! exp)
            (is (not (latched? main)))
            (sc/merge-from! main exp)
            (is (latched? main))
            (sc/add-doc main {:id {:value "m2" :type :string}})
            (sc/delete-docs main "id" "p1")
            (sc/commit! main)
            (assert-writer-blocks-intact main)
            (is (= [] (nested-ids main "alice" 5)))
            (.forceMerge (sc/->writer main) 1)
            (sc/commit! main)
            (assert-writer-blocks-intact main)
            (is (= [] (nested-ids main "alice" 5)) "p1's children must not join m2")
            (is (= [] (nested-ids main "bob" 1)))
            (is (= ["m1" "m1" "m2"] (search-ids main :all))
                "merge-from! is add-only, so the shared m1 arrived twice")
            (finally (sc/close! exp))))
        (finally (sc/close! main))))))

(deftest a-flat-write-in-flight-holds-off-the-first-block
  (testing "REGRESSION, silent corruption: the nested check ran apart from the
            write. A term delete of k that checked before the branch had any
            children, and wrote after k's first nested block landed, deleted
            k's root and left its children live. The write that sets the latch
            now waits for every flat write already past its check."
    (let [w (sc/create-index (under "idx") "main")
          writer (sc/->writer w)
          path-taken (promise)
          finish (promise)]
      (try
        (sc/add-doc w {:id {:value "k" :type :string}})
        (sc/commit! w)
        (let [flat (future
                     (.writeFlatOrNested
                      writer
                      (fn []
                        (deliver path-taken :flat)
                        @finish
                        ;; delete-docs' own flat write
                        (.deleteDocuments writer ^"[Lorg.apache.lucene.index.Term;"
                                          (into-array Term [(Term. "id" "k")])))
                      (fn [] (deliver path-taken :nested))))]
          (is (= :flat (deref path-taken 10000 ::timeout))
              "precondition: a branch without children takes the flat path")
          (let [block (future (sc/add-doc w (post "k" "kay" [["kim" 1] ["kai" 2]])))]
            (is (= ::waiting (deref block 200 ::waiting))
                "k's block waits for the flat write that already chose the term path")
            (is (not (latched? w)))
            (deliver finish true)
            @flat
            @block))
        (is (latched? w))
        (sc/commit! w)
        (assert-writer-blocks-intact w)
        (is (= ["k"] (search-ids w :all))
            "the delete ran first and removed the flat k; the new block is whole")
        (is (= ["k"] (nested-ids w "kim" 1)))
        (is (= 3 (sc/num-docs w)))
        (testing "and once the latch is set, a delete takes the block with it"
          (sc/delete-docs w "id" "k")
          (sc/commit! w)
          (assert-writer-blocks-intact w)
          (is (= 0 (sc/num-docs w))))
        (finally
          ;; A failed assertion above must not leave the flat write parked.
          (deliver finish true)
          (sc/close! w))))))

;; =============================================================================
;; Store-backed paths
;; =============================================================================

(defn- hot-post
  "Root `id` with `n` comments, all mentioning \"hot\" when `hot?` and none
  otherwise. Varied text gives varied scores, so score order is exercised."
  [id n hot?]
  {:id {:value id :type :string}
   :comments {:type :nested
              :value (vec (for [j (range n)]
                            {:body {:value (if hot?
                                             (nth ["hot" "hot hot" "hot and mild"] j)
                                             "cold")
                                    :type :text}
                             :n {:value j :type :int}}))}})

(defn- root-id [i] (format "r%02d" i))

(defn- page-through
  "Every candidate of `query-fn`'s query, following `:after` to exhaustion.

  The query is rebuilt for every page, as a caller holding only the
  continuation would: resuming requires the rebuilt query to be equal to the
  one the cursor recorded."
  [snap query-fn opts]
  (loop [after nil
         acc []
         pages 0]
    (let [{:keys [candidates continuation exhausted?]}
          (sc/candidate-page snap (query-fn) (assoc opts :after after))
          acc (into acc candidates)]
      (if exhausted?
        {:candidates acc :pages (inc pages)}
        (recur continuation acc (inc pages))))))

(deftest candidates-page-through-nested-roots-in-a-store
  (let [s (store-at (under "store"))
        c (under "cache")
        w (sc/open-store-index s c "main")
        hot-q #(sc/nested-query "comments" (sc/text-query "comments.body" "hot"))
        ;; Every fourth root has only cold comments.
        hot-on-main (set (map root-id (remove #(= 3 (mod % 4)) (range 12))))
        hot-on-exp (-> hot-on-main
                       (disj (root-id 0))
                       (conj (root-id 3) (root-id 12) (root-id 13)))]
    (try
      (doseq [i (range 12)]
        (sc/add-doc w (hot-post (root-id i) (inc (mod i 3)) (not= 3 (mod i 4)))))
      (sc/commit! w "roots")
      (assert-writer-blocks-intact w)
      (let [main-address (sc/snapshot-address w)
            exp (sc/fork w "exp")]
        (try
          (sc/update-doc exp "id" (root-id 0) (hot-post (root-id 0) 2 false))
          (sc/update-doc exp "id" (root-id 3) (hot-post (root-id 3) 3 true))
          (sc/add-doc exp (hot-post (root-id 12) 1 true))
          (sc/add-doc exp (hot-post (root-id 13) 2 true))
          (assert-writer-blocks-intact exp)
          (sc/commit! exp "exp edits")
          (doseq [[label address expected roots]
                  [["main" main-address hot-on-main 12]
                   ["exp" (sc/snapshot-address exp) hot-on-exp 14]]]
            (testing label
              (with-open [snap (sc/open-store-snapshot s c address)]
                (assert-blocks-intact (:reader snap))
                (is (= roots (sc/count-store-snapshot snap :all)))
                (is (= (count expected) (sc/count-store-snapshot snap (hot-q))))
                (testing "by score"
                  (let [{:keys [candidates pages]}
                        (page-through snap hot-q {:page-size 4 :fields [:id]
                                                  :query-id :hot})]
                    (is (= (sort expected) (ids candidates))
                        "every matching root, each exactly once")
                    (is (= 3 pages))
                    (is (apply >= (map :score candidates)))))
                (testing "by doc id"
                  (let [{:keys [candidates pages]}
                        (page-through snap hot-q {:page-size 4 :fields [:id]
                                                  :order :doc-id})]
                    (is (= (sort expected) (ids candidates)))
                    (is (= 3 pages))
                    (is (apply < (map :doc-id candidates)))))
                (testing "paging :all visits roots only"
                  (let [{:keys [candidates]}
                        (page-through snap (constantly :all) {:page-size 5})]
                    (is (= roots (count candidates)))
                    (is (every? #(get % "id") candidates)))))))
          (finally (sc/close! exp))))
      (finally (sc/close! w)))))

(deftest a-detached-generation-seals-nested-blocks
  (let [s (store-at (under "store"))
        c (under "cache")
        source (sc/open-store-index s c "source")
        count-at (fn [snap author stars]
                   (sc/count-store-snapshot
                    snap (sc/nested-query "comments" (comment-q author stars))))]
    (try
      (seed! source seed-posts)
      (sc/commit! source "base")
      (let [base (sc/snapshot-address source)
            generation (sc/begin-generation s c base
                                            {:workspace-id "nested-generation"})]
        (try
          (sc/add-doc generation (post "p4" "four" [["gus" 2]]))
          (assert-writer-blocks-intact generation)
          (sc/update-doc generation "id" "p1" (post "p1" "one" [["hal" 3]]))
          (assert-writer-blocks-intact generation)
          (sc/delete-docs generation "title" "two")
          (assert-writer-blocks-intact generation)
          (let [sealed (sc/seal-generation! generation "nested")]
            (with-open [snap (sc/open-store-snapshot s c sealed)]
              (assert-blocks-intact (:reader snap))
              (is (= ["p1" "p3" "p4"]
                     (ids (sc/search-store-snapshot snap :all {:limit 100}))))
              (is (= ["p1"] (ids (sc/search-store-snapshot
                                  snap (sc/nested-query "comments"
                                                        (comment-q "hal" 3))))))
              (is (= 1 (count-at snap "gus" 2)))
              (is (= 1 (count-at snap "alice" 1)))
              (is (= 0 (count-at snap "alice" 5)) "p1's old block is gone")
              (is (= 0 (count-at snap "carol" 3)) "p2 went with its child"))
            (with-open [snap (sc/open-store-snapshot s c base)]
              (assert-blocks-intact (:reader snap))
              (is (= 1 (count-at snap "alice" 5)) "the base generation is untouched")
              (is (= 1 (count-at snap "carol" 3)))
              (is (= 0 (count-at snap "gus" 2))))
            (sc/release-generation! generation))
          (finally (sc/close! generation))))
      (finally (sc/close! source)))))

;; =============================================================================
;; Shapes that are refused, and the pre-built path
;; =============================================================================

(deftest a-dotted-nested-field-name-is-refused
  (testing "a path segment IS a nesting level, so a :nested name with a dot
            would claim a level that has no documents; a nested query through
            it would join these children to some other object's"
    (let [w (sc/create-index (under "idx") "main")
          reply {:author {:value "b" :type :string}}
          on-root {:id {:value "p" :type :string}
                   :comments.replies {:type :nested :value [reply]}}
          in-child {:id {:value "p" :type :string}
                    :comments {:type :nested
                               :value [{:author {:value "a" :type :string}
                                        :replies.likes {:type :nested
                                                        :value [reply]}}]}}
          likes {:likes {:type :nested :value ["not a map"]}}
          deep-bad-value {:id {:value "p" :type :string}
                          :comments {:type :nested
                                     :value [{:replies {:type :nested :value [likes]}}]}}]
      (try
        (doseq [[label doc] [["on a root" on-root]
                             ["in a child" in-child]
                             ["an empty name" {:id {:value "p" :type :string}
                                               (keyword "") {:type :nested :value [reply]}}]]]
          (testing label
            (is (thrown-with-msg? ExceptionInfo #"contain no \"\.\"" (sc/add-doc w doc)))
            (is (thrown? ExceptionInfo (sc/update-doc w "id" "p" doc)))))
        (testing "a refusal three levels down"
          (is (thrown? ExceptionInfo (sc/add-doc w deep-bad-value))))
        (testing "a dotted name that is not :nested is a field name like any other"
          (sc/add-doc w {:id {:value "flat" :type :string}
                         :meta.source {:value "import" :type :string}})
          (is (= ["flat"] (search-ids w {:term [:meta.source "import"]}))))
        (sc/commit! w)
        (is (= 1 (sc/num-docs w))
            "every refused block was refused whole, not written up to the child that failed")
        (is (not (latched? w)))
        (finally (sc/close! w))))))

(deftest a-prebuilt-block-is-a-nested-root
  (let [w (sc/create-index (under "idx") "main")
        child prebuilt-child
        root prebuilt-root
        by-author #(search-ids w (sc/nested-query
                                  "comments" {:term [:comments.author %]}))]
    (try
      (sc/add-block w [(child "zoe") (child "yan") (root "b1")])
      (sc/add-block w [(child "yan") (root "b2")])
      (seed! w (take 1 seed-posts))
      (sc/commit! w)
      (assert-writer-blocks-intact w)
      (is (= ["b1"] (by-author "zoe")))
      (is (= ["b1" "b2"] (by-author "yan")))
      (is (= ["p1"] (by-author "alice")) "add-doc and add-block blocks interleave")
      (is (= ["b1" "b2" "p1"] (search-ids w :all)))
      (sc/delete-docs w "id" "b1")
      (sc/commit! w)
      (assert-writer-blocks-intact w)
      (is (= [] (by-author "zoe")))
      (is (= ["b2"] (by-author "yan")))
      (is (= 5 (sc/num-docs w)) "b2 1+1, p1 1+2")
      (finally (sc/close! w)))))

(deftest misshapen-blocks-are-refused
  (testing "REGRESSION, silent corruption: add-block wrote whatever sequence it
            was given, add-document took a lone child, and add-doc took a
            top-level _nested_path. Each left children with no root after
            them, which then joined the NEXT root written — someone else's —
            often with CheckJoinIndex reporting the index intact."
    (let [w (sc/create-index (under "idx") "main")
          by-author #(search-ids w (sc/nested-query
                                    "comments" {:term [:comments.author %]}))
          claims-reserved (assoc (post "r1" "one" [["mallory" 1]])
                                 :_nested_path {:value "comments" :type :string})]
      (try
        (doseq [[label block] [["root first" [(prebuilt-root "r1")
                                              (prebuilt-child "mallory")]]
                               ["no root" [(prebuilt-child "mallory")
                                           (prebuilt-child "eve")]]
                               ["empty" []]
                               ["a root before the last document"
                                [(prebuilt-child "mallory") (prebuilt-root "r1")
                                 (prebuilt-child "eve") (prebuilt-root "r2")]]]]
          (testing (str "add-block, " label)
            (is (thrown? ExceptionInfo (sc/add-block w block)))))
        (testing "add-document of a lone child"
          (is (thrown? ExceptionInfo (sc/add-document w (prebuilt-child "eve")))))
        (testing "a doc-map claiming the reserved field for its root"
          (is (thrown? ExceptionInfo (sc/add-doc w claims-reserved)))
          (is (thrown? ExceptionInfo (sc/update-doc w "id" "r1" claims-reserved))))
        (is (not (latched? w)) "every refusal came before the writer saw a document")
        (sc/add-doc w (post "r2" "two" [["bob" 2]]))
        (sc/commit! w)
        (assert-writer-blocks-intact w)
        (is (= 2 (sc/num-docs w)) "r2 and its child, nothing else")
        (is (= [] (by-author "mallory")) "no stray child joined r2")
        (is (= [] (by-author "eve")))
        (is (= ["r2"] (by-author "bob")))
        (is (= ["r2"] (search-ids w :all)))
        (finally (sc/close! w))))))

;; =============================================================================
;; Nested objects inside nested objects
;; =============================================================================

(defn- reply-spec [author]
  {:author {:value author :type :string}})

(defn- thread-comment
  "A :comments child by `author`, with one :replies child per reply author."
  [[author & replies]]
  {:author {:value author :type :string}
   :replies {:type :nested :value (mapv reply-spec replies)}})

(defn- thread
  "A root with one :comments child per [author & reply-authors]."
  [id comments]
  {:id {:value id :type :string}
   :title {:value id :type :string}
   :comments {:type :nested :value (mapv thread-comment comments)}})

(def ^:private seed-threads
  "t2 has a comment by alice and a reply by bob, but bob replied to carol —
  the post a nested query on replies inside one on comments must not find,
  and two nested queries side by side do."
  [["t1" [["alice" "bob"] ["carol"]]]
   ["t2" [["alice" "dave"] ["carol" "bob"]]]
   ["t3" [["erin"]]]
   ["t4" [["bob" "alice"]]]])

(def ^:private seed-thread-doc-count
  "t1 4, t2 5, t3 2, t4 3: roots, comments and replies."
  14)

(defn- seed-threads! [w threads]
  (doseq [[id comments] threads]
    (sc/add-doc w (thread id comments))))

(defn- replies-by
  "A nested query on replies by `author`; joined to roots at top level, to
  comments inside a nested query on comments."
  ([author] (replies-by author {}))
  ([author opts]
   (sc/nested-query :comments.replies {:term [:comments.replies.author author]} opts)))

(defn- comment-replied
  "Posts with a comment by `author` that has a reply by `replier`: the reply
  query inside the comment query, so both hold for ONE comment."
  ([author replier] (comment-replied author replier {}))
  ([author replier opts]
   (sc/nested-query :comments
                    (sc/bool-query [[{:term [:comments.author author]} :filter]
                                    [(replies-by replier) :filter]])
                    opts)))

(defn- inner-nested
  "The NestedQuery among `q`'s child query's clauses."
  ^NestedQuery [^NestedQuery q]
  (some #(when (instance? NestedQuery %) %)
        (map #(.query ^org.apache.lucene.search.BooleanClause %)
             (.clauses ^org.apache.lucene.search.BooleanQuery (.getChildQuery q)))))

(deftest a-nested-query-inside-a-nested-query-joins-to-its-objects
  (testing "ES semantics: the inner query's parents are the enclosing query's
            objects, so the reply must be under the SAME comment"
    (let [w (sc/create-index (under "idx") "main")]
      (try
        (seed-threads! w seed-threads)
        (sc/commit! w)
        (assert-writer-blocks-intact w)
        (is (= seed-thread-doc-count (sc/num-docs w)))
        (is (= ["t1"] (search-ids w (comment-replied "alice" "bob")))
            "t2 has alice's comment and bob's reply, but bob replied to carol")
        (is (= ["t1" "t2"]
               (search-ids w (sc/bool-query [[(sc/nested-query
                                               :comments {:term [:comments.author "alice"]})
                                              :filter]
                                             [(replies-by "bob") :filter]])))
            "side by side, the two conditions may hold in different comments")
        (is (= ["t2"] (search-ids w (comment-replied "carol" "bob"))))
        (is (= ["t4"] (search-ids w (comment-replied "bob" "alice"))))
        (is (= [] (search-ids w (comment-replied "erin" "bob"))))
        (testing "a top-level nested query on a deeper path joins to roots"
          (is (= ["t1" "t2"] (search-ids w (replies-by "bob"))))
          (is (= ["t4"] (search-ids w (replies-by "alice"))))
          (is (= [] (search-ids w (replies-by "erin"))) "erin wrote a comment, not a reply")
          (is (= [] (search-ids w (sc/nested-query :comments
                                                   {:term [:comments.replies.author "bob"]})))
              "each level's query matches that level's documents only"))
        (testing "an inner query under :must-not: comments by carol with no reply by bob"
          (is (= ["t1"]
                 (search-ids w (sc/nested-query
                                :comments
                                (sc/bool-query [[{:term [:comments.author "carol"]} :filter]
                                                [(replies-by "bob") :must-not]]))))))
        (testing "the binding is data on the query"
          (let [inner (replies-by "bob")
                outer (comment-replied "alice" "bob")]
            (is (nil? (.getParentPath ^NestedQuery inner)) "top level: the roots")
            (is (= "comments" (.getParentPath (inner-nested outer))))
            (is (= "comments.replies" (.getPath (inner-nested outer))))
            (let [built-on (sc/nested-query :comments
                                            (sc/bool-query
                                             [[{:term [:comments.author "alice"]} :filter]
                                              [inner :filter]]))]
              (is (= outer built-on))
              (is (nil? (.getParentPath ^NestedQuery inner))
                  "binding builds a new query and leaves the one it was given alone"))))
        (testing "every score mode, at either level, matches the same roots"
          (doseq [outer [:avg :max :min :sum :none]
                  inner [:avg :max :min :sum :none]]
            (is (= ["t1"]
                   (search-ids w (sc/nested-query
                                  :comments
                                  (sc/bool-query [[{:term [:comments.author "alice"]} :must]
                                                  [(replies-by "bob" {:score-mode inner}) :must]])
                                  {:score-mode outer})))
                (str outer " over " inner))))
        (is (= [] (search-ids w {:term [:comments.replies.author "bob"]}))
            "a reply is a child like any other: no ordinary query returns it")
        (finally (sc/close! w))))))

(defn- tree
  "A root whose :a objects each hold :b objects, each holding :c objects:
  `as` is [[a-name [[b-name [c-value ...]] ...]] ...]."
  [id as]
  (let [nested (fn [f xs] {:type :nested :value (mapv f xs)})
        c (fn [v] {:v {:value v :type :string}})
        b (fn [[b-name cs]] {:name {:value b-name :type :string} :c (nested c cs)})
        a (fn [[a-name bs]] {:name {:value a-name :type :string} :b (nested b bs)})]
    {:id {:value id :type :string}
     :a (nested a as)}))

(def ^:private seed-trees
  [["R1" [["a1" [["b1" ["x"]] ["b2" ["y"]]]]
          ["a2" [["b1" ["y"]]]]]]
   ["R2" [["a1" [["b1" ["y"]]]]
          ["a2" [["b2" ["x"]]]]]]
   ["R3" [["a1" [["b2" ["x" "y"]]]]]]])

(defn- tree-q
  "Roots with an a named `a` holding a b named `b` holding a c with value `v`:
  three levels, each nested in the one above."
  [a b v]
  (sc/nested-query
   :a (sc/bool-query
       [[{:term [:a.name a]} :filter]
        [(sc/nested-query
          :a.b (sc/bool-query
                [[{:term [:a.b.name b]} :filter]
                 [(sc/nested-query :a.b.c {:term [:a.b.c.v v]}) :filter]]))
         :filter]])))

(defn- skip-q
  "Roots with an a named `a` holding, at any b, a c with value `v`: the c query
  directly inside the a query, skipping the b level."
  [a v]
  (sc/nested-query :a (sc/bool-query [[{:term [:a.name a]} :filter]
                                      [(sc/nested-query :a.b.c {:term [:a.b.c.v v]})
                                       :filter]])))

(deftest nesting-goes-three-levels-deep-and-may-skip-one
  (let [w (sc/create-index (under "idx") "main")]
    (try
      (doseq [[id as] seed-trees]
        (sc/add-doc w (tree id as)))
      (sc/commit! w)
      (assert-writer-blocks-intact w)
      (is (= 21 (sc/num-docs w)) "R1 1+2+3+3, R2 1+2+2+2, R3 1+1+1+2")
      (testing "every level's condition holds along one chain of objects"
        (is (= ["R1"] (search-ids w (tree-q "a1" "b1" "x"))))
        (is (= ["R3"] (search-ids w (tree-q "a1" "b2" "x"))))
        (is (= ["R1" "R3"] (search-ids w (tree-q "a1" "b2" "y"))))
        (is (= ["R2"] (search-ids w (tree-q "a2" "b2" "x"))))
        (is (= [] (search-ids w (tree-q "a2" "b1" "x")))
            "R1 has a2/b1 and a1/b1/x, and R2 a2/b2/x, but no a2/b1/x"))
      (testing "the c query inside the a query joins each c to its own a"
        (let [q (skip-q "a1" "x")]
          (is (= "a" (.getParentPath (inner-nested q))))
          (is (= ["R1" "R3"] (search-ids w q))))
        (is (= ["R2"] (search-ids w (skip-q "a2" "x")))
            "R1's a2 holds only y, and its x is under a1")
        (is (= ["R1" "R2" "R3"] (search-ids w (skip-q "a1" "y")))))
      (testing "a top-level query two levels down joins straight to the roots"
        (is (= ["R1" "R2" "R3"]
               (search-ids w (sc/nested-query :a.b.c {:term [:a.b.c.v "x"]})))))
      (testing "a top-level query one level down, with a nested query inside it"
        (is (= ["R2" "R3"]
               (search-ids w (sc/nested-query
                              :a.b (sc/bool-query
                                    [[{:term [:a.b.name "b2"]} :filter]
                                     [(sc/nested-query :a.b.c {:term [:a.b.c.v "x"]})
                                      :filter]]))))))
      (finally (sc/close! w)))))

(deftest a-doc-map-is-written-in-post-order
  (testing "each object right after its own children, several nested fields
            per level in doc-map order, the root last"
    (let [w (sc/create-index (under "idx") "main")
          who (fn [v] {:by {:value v :type :string}})]
      (try
        (sc/add-doc w {:id {:value "d" :type :string}
                       :comments {:type :nested
                                  :value [{:by {:value "c1" :type :string}
                                           :replies {:type :nested
                                                     :value [(who "r1") (who "r2")]}
                                           :likes {:type :nested :value [(who "l1")]}}
                                          {:by {:value "c2" :type :string}
                                           :likes {:type :nested :value [(who "l2")]}}]}
                       :reviews {:type :nested
                                 :value [{:by {:value "v1" :type :string}
                                          :notes {:type :nested :value [(who "n1")]}}]}})
        (sc/commit! w)
        (with-open [r (sc/snapshot w)]
          (assert-blocks-intact r)
          (is (= [["comments.replies" "comments.replies" "comments.likes" "comments"
                   "comments.likes" "comments"
                   "reviews.notes" "reviews"
                   nil]]
                 (mapv #(segment-paths (.reader ^LeafReaderContext %)) (.leaves r)))))
        (let [under-comment (fn [c path v]
                              (search-ids w (sc/nested-query
                                             :comments
                                             (sc/bool-query
                                              [[{:term [:comments.by c]} :filter]
                                               [(sc/nested-query
                                                 path {:term [(str path ".by") v]})
                                                :filter]]))))]
          (is (= ["d"] (under-comment "c1" "comments.replies" "r2")))
          (is (= ["d"] (under-comment "c1" "comments.likes" "l1")))
          (is (= ["d"] (under-comment "c2" "comments.likes" "l2")))
          (is (= [] (under-comment "c2" "comments.likes" "l1")) "l1 is c1's")
          (is (= [] (under-comment "c2" "comments.replies" "r1")) "c2 has no replies")
          (is (= ["d"] (search-ids w (sc/nested-query
                                      :reviews
                                      (sc/bool-query
                                       [[{:term [:reviews.by "v1"]} :filter]
                                        [(sc/nested-query :reviews.notes
                                                          {:term [:reviews.notes.by "n1"]})
                                         :filter]]))))))
        (finally (sc/close! w))))))

(deftest binding-refuses-what-it-cannot-join
  (let [inner (replies-by "bob")
        in-comments #(sc/nested-query :comments %)]
    (testing "an inner path must be under the enclosing one"
      (doseq [[label path] [["another field" "reviews.replies"]
                            ["the same path" "comments"]
                            ["a longer field name, not a deeper level" "commentsx.replies"]]]
        (testing label
          (is (thrown-with-msg?
               IllegalArgumentException
               (re-pattern (str "\"" (java.util.regex.Pattern/quote path) "\".*\"comments\""))
               (in-comments (sc/nested-query path {:term [:x "y"]}))))))
      (testing "a level above"
        (is (thrown-with-msg?
             IllegalArgumentException #"\"comments\".*\"comments.replies\""
             (sc/nested-query :comments.replies
                              (sc/nested-query :comments {:term [:x "y"]}))))))
    (testing "a nested query reached through a query binding cannot rebuild
              throws, rather than staying joined to the roots"
      (doseq [[label wrapper] [["IndexOrDocValuesQuery" (IndexOrDocValuesQuery. inner inner)]
                               ["a block join, which visits as a leaf"
                                (ToParentBlockJoinQuery. inner sc/roots-bitset ScoreMode/None)]
                               ["a child block join"
                                (ToChildBlockJoinQuery. inner sc/roots-bitset)]
                               ["under a :must-not, inside one"
                                (IndexOrDocValuesQuery.
                                 (sc/bool-query [[(MatchAllDocsQuery.) :filter]
                                                 [inner :must-not]])
                                 (MatchAllDocsQuery.))]]]
        (testing label
          (is (thrown-with-msg? IllegalArgumentException #"binding cannot see through"
                                (in-comments (sc/bool-query [[{:term [:comments.author "alice"]}
                                                              :filter]
                                                             [wrapper :filter]])))))))
    (testing "malformed paths"
      (doseq [path ["" "." ".comments" "comments." "comments..replies"]]
        (is (thrown? ExceptionInfo (sc/nested-query path {:term [:x "y"]})) (pr-str path))
        (is (thrown? IllegalArgumentException
                     (NestedQuery. path (MatchAllDocsQuery.) ScoreMode/Avg))
            (pr-str path)))
      (is (thrown? IllegalArgumentException
                   (NestedQuery. "comments.replies" (MatchAllDocsQuery.) ScoreMode/Avg
                                 "replies" nil))
          "an explicit parent path must be above the path")
      (is (thrown? IllegalArgumentException (NestedQuery/parentFilter "comments."))))))

(deftest supported-wrappers-are-rebuilt-around-the-bound-query
  (let [w (sc/create-index (under "idx") "main")]
    (try
      (seed-threads! w seed-threads)
      (sc/commit! w)
      (doseq [[label wrap] [["boost" #(BoostQuery. % 2.0)]
                            ["constant score" #(ConstantScoreQuery. %)]
                            ["dis-max" #(DisjunctionMaxQuery. [% (replies-by "zed")] 0.1)]
                            ["all three, in a bool"
                             #(sc/bool-query [[(BoostQuery.
                                                (ConstantScoreQuery.
                                                 (DisjunctionMaxQuery. [% (replies-by "zed")]
                                                                       0.0))
                                                3.0)
                                               :must]])]]]
        (testing label
          (is (= ["t1"]
                 (search-ids w (sc/nested-query
                                :comments
                                (sc/bool-query [[{:term [:comments.author "alice"]} :filter]
                                                [(wrap (replies-by "bob")) :must]])))))))
      (testing "at top level any wrapper will do: the roots are the parents either way"
        (is (= ["t1" "t2"] (search-ids w (IndexOrDocValuesQuery. (replies-by "bob")
                                                                 (replies-by "bob"))))))
      (finally (sc/close! w)))))

(deftest a-nested-query-is-a-value
  (testing "equal when built alike, so a continuation can recognize its query"
    (is (= (comment-replied "alice" "bob") (comment-replied "alice" "bob")))
    (is (= (hash (comment-replied "alice" "bob")) (hash (comment-replied "alice" "bob"))))
    (doseq [[label other] [["another value" (comment-replied "alice" "dave")]
                           ["another outer score mode"
                            (comment-replied "alice" "bob" {:score-mode :max})]
                           ["another inner score mode"
                            (sc/nested-query :comments
                                             (sc/bool-query
                                              [[{:term [:comments.author "alice"]} :filter]
                                               [(replies-by "bob" {:score-mode :sum}) :filter]]))]]]
      (is (not= (comment-replied "alice" "bob") other) label))
    (let [child (TermQuery. (Term. "comments.replies.author" "bob"))]
      (is (not= (NestedQuery. "comments.replies" child ScoreMode/Avg)
                (NestedQuery. "comments.replies" child ScoreMode/Avg "comments" nil))
          "the parent path")
      (is (= (NestedQuery. "comments.replies" child ScoreMode/Avg "comments" {:size 3})
             (NestedQuery. "comments.replies" child ScoreMode/Avg "comments" {:size 3})))
      (is (not= (NestedQuery. "comments.replies" child ScoreMode/Avg "comments" {:size 3})
                (NestedQuery. "comments.replies" child ScoreMode/Avg "comments" nil))
          "inner hits")))
  (testing "it rewrites to the block join, over one shared parents filter per level"
    (is (identical? sc/roots-query (NestedQuery/rootsQuery)))
    (is (identical? sc/roots-bitset (NestedQuery/rootsFilter)))
    (is (identical? sc/roots-bitset (NestedQuery/parentFilter nil)))
    (is (identical? (NestedQuery/parentFilter "comments") (NestedQuery/parentFilter "comments")))
    (let [w (sc/create-index (under "idx") "main")]
      (try
        (seed-threads! w seed-threads)
        (sc/commit! w)
        (with-open [r (sc/snapshot w)]
          (let [searcher (IndexSearcher. r)
                q (inner-nested (comment-replied "alice" "bob"))
                ^ToParentBlockJoinQuery join (.rewrite ^NestedQuery q searcher)]
            (is (instance? ToParentBlockJoinQuery join))
            (is (= (.childLevelQuery ^NestedQuery q) (.getChildQuery join)))
            (is (= (ToParentBlockJoinQuery. (.childLevelQuery ^NestedQuery q)
                                            (NestedQuery/parentFilter "comments")
                                            ScoreMode/Avg)
                   join))
            (is (= 2 (.count searcher (replies-by "bob"))) "IndexSearcher rewrites it first")))
        (finally (sc/close! w))))))

;; --- Writes on multi-level blocks ---

(defn- whole-blocks
  "The roots `q` matches and every document of their blocks, from public
  pieces: what a BranchIndexWriter caller deletes to remove blocks whole."
  ^Query [q]
  (let [roots (sc/bool-query [[q :filter] [sc/roots-query :filter]])]
    (sc/bool-query [[roots :should]
                    [(ToChildBlockJoinQuery. roots sc/roots-bitset) :should]])))

(defn- prebuilt-nested
  "A child on `path` built by hand, with `by` under `<path>.by`."
  ^Document [^String path ^String by]
  (doto (Document.)
    (.add (StringField. sc/nested-path-field path Field$Store/NO))
    (.add (StringField. (str path ".by") by Field$Store/YES))))

(deftest deleting-a-root-takes-its-whole-tree
  (doseq [[label delete!] [["delete-docs" #(sc/delete-docs % "id" "t2")]
                           ["delete-query" #(sc/delete-query % (TermQuery. (Term. "title" "t2")))]
                           ;; Lucene's delete path rewrites the query, so the
                           ;; NestedQuery becomes its block join there too.
                           ["delete-query by a nested query inside a nested query"
                            #(sc/delete-query % (comment-replied "carol" "bob"))]
                           ["BranchIndexWriter.deleteDocuments, the blocks a nested query finds"
                            #(.deleteDocuments ^BranchIndexWriter (sc/->writer %)
                                               ^"[Lorg.apache.lucene.search.Query;"
                                               (into-array Query [(whole-blocks
                                                                   (comment-replied
                                                                    "carol" "bob"))]))]]]
    (testing label
      (let [w (sc/create-index (under label) "main")]
        (try
          (seed-threads! w seed-threads)
          (sc/commit! w)
          (is (latched? w))
          (delete! w)
          (sc/commit! w)
          (assert-writer-blocks-intact w)
          (is (= ["t1" "t3" "t4"] (search-ids w :all)))
          (is (= (- seed-thread-doc-count 5) (sc/num-docs w)) "t2, its comments and their replies")
          (is (= [] (search-ids w (replies-by "dave"))))
          (.forceMerge (sc/->writer w) 1)
          (sc/commit! w)
          (assert-writer-blocks-intact w)
          (is (= [] (search-ids w (replies-by "dave"))) "t2's replies must not re-parent onto t3")
          (is (= [] (search-ids w (comment-replied "carol" "bob"))))
          (is (= ["t1"] (search-ids w (replies-by "bob"))))
          (is (= ["t1"] (search-ids w (comment-replied "alice" "bob"))))
          (is (= ["t4"] (search-ids w (comment-replied "bob" "alice"))))
          (is (= (- seed-thread-doc-count 5) (sc/num-docs w)))
          (finally (sc/close! w))))))
  (testing "a value only a reply carries deletes nothing"
    (let [w (sc/create-index (under "reply-only") "main")]
      (try
        (seed-threads! w seed-threads)
        (sc/commit! w)
        (sc/delete-docs w "comments.replies.author" "bob")
        (sc/delete-query w (TermQuery. (Term. "comments.replies.author" "bob")))
        (sc/commit! w)
        (assert-writer-blocks-intact w)
        (is (= ["t1" "t2" "t3" "t4"] (search-ids w :all)))
        (is (= seed-thread-doc-count (sc/num-docs w)))
        (is (= ["t1"] (search-ids w (comment-replied "alice" "bob"))))
        (finally (sc/close! w))))))

(deftest updating-a-root-replaces-its-whole-tree
  (testing "update-doc"
    (let [w (sc/create-index (under "update-doc") "main")]
      (try
        (seed-threads! w seed-threads)
        (sc/commit! w)
        (sc/update-doc w "id" "t1" (thread "t1" [["zoe" "yan" "xi"]]))
        (sc/commit! w)
        (assert-writer-blocks-intact w)
        (is (= [] (search-ids w (comment-replied "alice" "bob"))))
        (is (= ["t1"] (search-ids w (comment-replied "zoe" "xi"))))
        (is (= ["t2"] (search-ids w (replies-by "bob"))) "t1's old reply went with it")
        (is (= (+ seed-thread-doc-count -4 4) (sc/num-docs w)))
        (sc/update-doc w "id" "t3" {:id {:value "t3" :type :string}})
        (sc/commit! w)
        (assert-writer-blocks-intact w)
        (.forceMerge (sc/->writer w) 1)
        (sc/commit! w)
        (assert-writer-blocks-intact w)
        (is (= ["t1" "t2" "t3" "t4"] (search-ids w :all)))
        (is (= ["t1"] (search-ids w (comment-replied "zoe" "yan"))))
        (is (= ["t2"] (search-ids w (comment-replied "carol" "bob"))))
        (is (= [] (search-ids w (sc/nested-query :comments {:term [:comments.author "erin"]}))))
        (is (= (+ seed-thread-doc-count -1) (sc/num-docs w)) "t3 lost its one comment")
        (finally (sc/close! w)))))
  (testing "BranchIndexWriter.updateDocuments, keyed by a nested query: Lucene
            rewrites it on the delete side"
    (let [w (sc/create-index (under "update-documents") "main")]
      (try
        (seed-threads! w seed-threads)
        (sc/commit! w)
        (let [^Iterable block [(prebuilt-nested "comments.replies" "kim")
                               (prebuilt-nested "comments" "kai")
                               (prebuilt-root "t1")]]
          (.updateDocuments ^BranchIndexWriter (sc/->writer w)
                            (whole-blocks (comment-replied "alice" "bob"))
                            block))
        (sc/commit! w)
        (assert-writer-blocks-intact w)
        (is (= ["t1" "t2" "t3" "t4"] (search-ids w :all)))
        (is (= [] (search-ids w (comment-replied "alice" "bob"))))
        (is (= ["t1"] (search-ids w (sc/nested-query
                                     :comments
                                     (sc/bool-query
                                      [[{:term [:comments.by "kai"]} :filter]
                                       [(sc/nested-query :comments.replies
                                                         {:term [:comments.replies.by "kim"]})
                                        :filter]])))))
        (is (= (+ seed-thread-doc-count -4 3) (sc/num-docs w)))
        (finally (sc/close! w))))))

(deftest fork-and-merge-carry-multi-level-blocks
  (let [main (sc/create-index (under "idx") "main")]
    (try
      (seed-threads! main (take 2 seed-threads))
      (sc/commit! main)
      (assert-writer-blocks-intact main)
      (let [exp (sc/fork main "exp")]
        (try
          (is (latched? exp))
          (sc/update-doc exp "id" "t1" (thread "t1" [["alice" "eve"] ["bob" "bob"]]))
          (sc/add-doc exp (thread "t5" [["fay" "gil" "hal"]]))
          (sc/commit! exp)
          (assert-writer-blocks-intact exp)
          (testing "the fork sees its blocks"
            (is (= ["t1"] (search-ids exp (comment-replied "alice" "eve"))))
            (is (= [] (search-ids exp (comment-replied "alice" "bob"))))
            (is (= ["t1" "t2"] (search-ids exp (replies-by "bob"))))
            (is (= ["t5"] (search-ids exp (comment-replied "fay" "hal")))))
          (testing "the parent is unchanged"
            (assert-writer-blocks-intact main)
            (is (= ["t1"] (search-ids main (comment-replied "alice" "bob"))))
            (is (= [] (search-ids main (comment-replied "alice" "eve"))))
            (is (= 9 (sc/num-docs main))))
          (testing "merge-from! brings them whole"
            (sc/merge-from! main exp)
            (assert-writer-blocks-intact main)
            (is (= ["t1" "t1" "t2" "t2"]
                   (search-ids main (sc/nested-query :comments
                                                     {:term [:comments.author "alice"]})))
                "add-only: main's t1 and t2, and exp's new t1 and its copy of t2")
            (is (= ["t1"] (search-ids main (comment-replied "alice" "eve"))))
            (is (= ["t5"] (search-ids main (comment-replied "fay" "gil"))))
            (.forceMerge (sc/->writer main) 1)
            (sc/commit! main)
            (assert-writer-blocks-intact main)
            (is (= ["t1"] (search-ids main (comment-replied "alice" "eve"))))
            (is (= ["t1"] (search-ids main (comment-replied "alice" "bob"))))
            (is (= ["t2" "t2"] (search-ids main (comment-replied "carol" "bob")))
                "exp's copy of t2 arrived with its replies")
            (is (= ["t5"] (search-ids main (comment-replied "fay" "hal")))))
          (finally (sc/close! exp))))
      (finally (sc/close! main)))))

(deftest a-prebuilt-block-nests-in-post-order
  (let [w (sc/create-index (under "idx") "main")
        child prebuilt-nested
        replied #(search-ids w (sc/nested-query
                                :comments
                                (sc/bool-query
                                 [[{:term [:comments.by %1]} :filter]
                                  [(sc/nested-query :comments.replies
                                                    {:term [:comments.replies.by %2]})
                                   :filter]])))]
    (try
      (testing "a child whose parent would not be found is refused"
        (doseq [[label block] [["a reply after its comment"
                                [(child "comments" "c") (child "comments.replies" "r")
                                 (prebuilt-root "x")]]
                               ["a reply with no comment"
                                [(child "comments.replies" "r") (prebuilt-root "x")]]
                               ["a grandparent's document before the parent"
                                [(child "a.b.c" "c") (child "a" "a") (child "a.b" "b")
                                 (prebuilt-root "x")]]
                               ["an empty path segment"
                                [(child "comments..replies" "r") (child "comments" "c")
                                 (prebuilt-root "x")]]
                               ["the field twice"
                                [(doto (prebuilt-nested "comments" "c")
                                   (.add (StringField. sc/nested-path-field "reviews"
                                                       Field$Store/NO)))
                                 (prebuilt-root "x")]]]]
          (testing label
            (is (thrown? ExceptionInfo (sc/add-block w block)))))
        (is (not (latched? w)) "refused before the writer saw a document"))
      (sc/add-block w [(child "comments.replies" "r1") (child "comments.replies" "r2")
                       (child "comments" "c1")
                       (child "comments" "c2")
                       (child "comments.replies" "r3") (child "comments" "c3")
                       (prebuilt-root "b1")])
      (testing "an unrelated document between a child and its parent changes no join"
        (sc/add-block w [(child "comments.replies" "r4") (child "reviews" "v")
                         (child "comments" "c4") (prebuilt-root "b2")]))
      (sc/commit! w)
      (assert-writer-blocks-intact w)
      (is (= ["b1"] (replied "c1" "r2")))
      (is (= [] (replied "c2" "r3")) "c2 has no replies")
      (is (= ["b1"] (replied "c3" "r3")))
      (is (= ["b2"] (replied "c4" "r4")))
      (is (= ["b2"] (search-ids w (sc/nested-query :reviews {:term [:reviews.by "v"]}))))
      (finally (sc/close! w)))))

(deftest the-integrity-check-sees-every-level
  (testing "a reply with no comment after it in its block: CheckJoinIndex over
            the roots passes, the per-level check does not, and a reply query
            inside a comment query gives the reply to the NEXT root's comment"
    (let [w (sc/create-index (under "idx") "main")]
      (try
        ;; Straight to BranchIndexWriter, past add-block's check.
        (.addDocuments ^BranchIndexWriter (sc/->writer w)
                       [(prebuilt-nested "comments" "c1")
                        (prebuilt-nested "comments.replies" "r1")
                        (prebuilt-root "b1")])
        (sc/add-block w [(prebuilt-nested "comments" "c2") (prebuilt-root "b2")])
        (sc/commit! w)
        (.forceMerge (sc/->writer w) 1)
        (sc/commit! w)
        (with-open [r (sc/snapshot w)]
          (is (nil? (CheckJoinIndex/check r sc/roots-bitset)) "CheckJoinIndex passes it")
          (is (= "child 1 on comments.replies meets a root at 2 before its parent at 3"
                 (block-integrity r))))
        (is (= ["b1"] (search-ids w (sc/nested-query :comments.replies
                                                     {:term [:comments.replies.by "r1"]})))
            "joined to the roots, it is still b1's")
        (is (= ["b2"] (search-ids w (sc/nested-query
                                     :comments
                                     (sc/bool-query
                                      [[{:term [:comments.by "c2"]} :filter]
                                       [(sc/nested-query :comments.replies
                                                         {:term [:comments.replies.by "r1"]})
                                        :filter]]))))
            "joined to comments, it is c2's, in another root's block")
        (finally (sc/close! w))))))

;; --- Store-backed paths, multi-level ---

(def ^:private reply-bodies ["hot" "hot hot" "hot and mild"])

(defn- hot-comments
  "Root `i`'s comments, as [[comment-body [reply-body ...]] ...]: 1 to 3
  comments, warm or cool, with 1 to 3 replies each, hot or cold. Varied text
  gives varied scores, so score order is exercised."
  [i]
  (vec (for [j (range (inc (mod i 3)))]
         [(if (= (zero? j) (even? (quot i 3))) "warm" "cool")
          (vec (for [k (range (inc j))]
                 (if (= 1 (mod (+ i j) 4)) "cold" (nth reply-bodies k))))])))

(defn- hot-thread [id comments]
  {:id {:value id :type :string}
   :comments {:type :nested
              :value (vec (for [[body replies] comments]
                            {:body {:value body :type :text}
                             :replies {:type :nested
                                       :value (vec (for [r replies]
                                                     {:body {:value r :type :text}}))}}))}})

(defn- hot? [s] (str/includes? s "hot"))

(defn- hot-reply-q
  "Roots with a hot reply under any comment."
  []
  (sc/nested-query :comments.replies (sc/text-query "comments.replies.body" "hot")))

(defn- warm-hot-q
  "Roots with a warm comment that has a hot reply."
  []
  (sc/nested-query :comments
                   (sc/bool-query [[(sc/text-query "comments.body" "warm") :must]
                                   [(hot-reply-q) :must]])
                   {:score-mode :max}))

(def ^:private hot-queries
  "[label query-fn matches? comments]"
  [["a top-level query on replies" hot-reply-q
    (fn [comments] (some (fn [[_ replies]] (some hot? replies)) comments))]
   ["a reply query inside a comment query" warm-hot-q
    (fn [comments] (some (fn [[body replies]] (and (= "warm" body) (some hot? replies)))
                         comments))]])

(deftest candidates-page-through-multi-level-roots-in-a-store
  (let [s (store-at (under "store"))
        c (under "cache")
        w (sc/open-store-index s c "main")
        on-main (into (sorted-map) (map (juxt root-id hot-comments)) (range 12))
        on-exp (assoc on-main
                      (root-id 0) [["warm" ["cold"]]]
                      (root-id 1) [["warm" ["hot"]] ["cool" ["cold"]]]
                      (root-id 12) [["cool" ["hot"]]]
                      (root-id 13) [["warm" ["hot hot"]]])]
    (try
      (doseq [[id comments] on-main]
        (sc/add-doc w (hot-thread id comments)))
      (sc/commit! w "roots")
      (assert-writer-blocks-intact w)
      (let [main-address (sc/snapshot-address w)
            exp (sc/fork w "exp")]
        (try
          (doseq [id [(root-id 0) (root-id 1)]]
            (sc/update-doc exp "id" id (hot-thread id (on-exp id))))
          (doseq [id [(root-id 12) (root-id 13)]]
            (sc/add-doc exp (hot-thread id (on-exp id))))
          (assert-writer-blocks-intact exp)
          (sc/commit! exp "exp edits")
          (doseq [[label address data] [["main" main-address on-main]
                                        ["exp" (sc/snapshot-address exp) on-exp]]]
            (testing label
              (with-open [snap (sc/open-store-snapshot s c address)]
                (assert-blocks-intact (:reader snap))
                (is (= (count data) (sc/count-store-snapshot snap :all)))
                (doseq [[qlabel query-fn matches?] hot-queries
                        :let [expected (vec (sort (keep (fn [[id cs]] (when (matches? cs) id))
                                                        data)))
                              pages (max 1 (quot (+ 3 (count expected)) 4))]]
                  (testing qlabel
                    (is (< 0 (count expected) (count data)) "precondition: a selective query")
                    (is (= (count expected) (sc/count-store-snapshot snap (query-fn))))
                    (testing "by score"
                      (let [{:keys [candidates] :as paged}
                            (page-through snap query-fn {:page-size 4 :fields [:id]
                                                         :query-id :hot})]
                        (is (= expected (ids candidates)) "every matching root, each once")
                        (is (= pages (:pages paged)))
                        (is (apply >= (map :score candidates)))))
                    (testing "by doc id"
                      (let [{:keys [candidates] :as paged}
                            (page-through snap query-fn {:page-size 4 :fields [:id]
                                                         :order :doc-id})]
                        (is (= expected (ids candidates)))
                        (is (= pages (:pages paged)))
                        (is (apply < (map :doc-id candidates)))))))
                (testing "the two sets differ: one comment holding both is not
                          a warm comment beside a hot reply"
                  (is (not= (sc/count-store-snapshot snap (hot-reply-q))
                            (sc/count-store-snapshot snap (warm-hot-q)))))
                (testing "a continuation belongs to its query, inner levels included"
                  (let [{:keys [continuation]} (sc/candidate-page snap (warm-hot-q)
                                                                  {:page-size 1})]
                    (is continuation)
                    (is (seq (:candidates (sc/candidate-page snap (warm-hot-q)
                                                             {:page-size 1
                                                              :after continuation}))))
                    (doseq [other [(hot-reply-q)
                                   (sc/nested-query
                                    :comments
                                    (sc/bool-query
                                     [[(sc/text-query "comments.body" "warm") :must]
                                      [(sc/nested-query :comments.replies
                                                        (sc/text-query "comments.replies.body"
                                                                       "hot")
                                                        {:score-mode :min})
                                       :must]])
                                    {:score-mode :max})]]
                      (is (thrown? ExceptionInfo
                                   (sc/candidate-page snap other {:page-size 1
                                                                  :after continuation})))))))))
          (finally (sc/close! exp))))
      (finally (sc/close! w)))))

(deftest a-detached-generation-seals-multi-level-blocks
  (let [s (store-at (under "store"))
        c (under "cache")
        source (sc/open-store-index s c "source")
        count-at (fn [snap author replier]
                   (sc/count-store-snapshot snap (comment-replied author replier)))]
    (try
      (seed-threads! source seed-threads)
      (sc/commit! source "base")
      (let [base (sc/snapshot-address source)
            generation (sc/begin-generation s c base
                                            {:workspace-id "multi-level-generation"})]
        (try
          (sc/add-doc generation (thread "t5" [["gus" "hal"]]))
          (assert-writer-blocks-intact generation)
          (sc/update-doc generation "id" "t1" (thread "t1" [["alice" "ivy"]]))
          (assert-writer-blocks-intact generation)
          (sc/delete-query generation (comment-replied "carol" "bob"))
          (assert-writer-blocks-intact generation)
          (let [sealed (sc/seal-generation! generation "nested")]
            (with-open [snap (sc/open-store-snapshot s c sealed)]
              (assert-blocks-intact (:reader snap))
              (is (= ["t1" "t3" "t4" "t5"]
                     (ids (sc/search-store-snapshot snap :all {:limit 100}))))
              (is (= 1 (count-at snap "gus" "hal")))
              (is (= 1 (count-at snap "alice" "ivy")))
              (is (= 0 (count-at snap "alice" "bob")) "t1's old tree is gone")
              (is (= 0 (sc/count-store-snapshot snap (replies-by "dave")))
                  "t2 went with its comments' replies")
              (is (= 1 (count-at snap "bob" "alice"))))
            (with-open [snap (sc/open-store-snapshot s c base)]
              (assert-blocks-intact (:reader snap))
              (is (= 1 (count-at snap "alice" "bob")) "the base generation is untouched")
              (is (= 1 (count-at snap "carol" "bob")))
              (is (= 0 (count-at snap "gus" "hal"))))
            (sc/release-generation! generation))
          (finally (sc/close! generation))))
      (finally (sc/close! source)))))
