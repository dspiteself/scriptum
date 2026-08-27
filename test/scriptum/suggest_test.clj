(ns scriptum.suggest-test
  "Lucene's suggest fields over a branched index.

  What these pin down is not that a codec option is accepted but that COMPLETION
  POSTINGS COMPOSE WITH BRANCHING: a fork reads its parent's suggestions out of
  segments it shares rather than copies, its own additions stay off the parent,
  and a reader opened at an older generation suggests what that generation held.

  `lucene-suggest` is on the test classpath only. scriptum names no completion
  format itself — the codec below is the caller's, which is the whole point of
  the option."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.java.io :as io]
            [konserve.store :as kstore]
            [scriptum.core :as sc])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]
           [org.apache.lucene.analysis.standard StandardAnalyzer]
           [org.apache.lucene.codecs Codec]
           [org.apache.lucene.codecs.lucene103 Lucene103Codec]
           [org.apache.lucene.document Document Field$Store StringField]
           [org.apache.lucene.index DirectoryReader IndexWriterConfig Term]
           [org.apache.lucene.search.suggest.document
            Completion101PostingsFormat PrefixCompletionQuery SuggestField
            SuggestIndexSearcher TopSuggestDocs$SuggestScoreDoc]))

(defn- temp-dir []
  (str (Files/createTempDirectory "scriptum-suggest-test-"
                                  (make-array FileAttribute 0))))

(defn- delete-dir-recursive [path]
  (let [dir (java.io.File. path)]
    (when (.exists dir)
      (doseq [f (reverse (file-seq dir))]
        (.delete f)))))

(defn- store-at
  "A konserve store with an identity — `connect-store`, never `connect-fs-store`,
  because only the former attaches the UUID scriptum's GC guard needs."
  [path]
  ;; The parent only: `create-store` refuses a path that already exists.
  (.mkdirs (.getParentFile (io/file path)))
  (kstore/create-store {:backend :file :path path :id (random-uuid)} {:sync? true}))

(def ^:private suggest-field "suggest_name")

(defn- completion-codec
  "The caller-supplied codec a SuggestField needs.

  Lucene's default codec answers `getPostingsFormatForField` with an ordinary
  postings format, and a SuggestField written through one is accepted and then
  unqueryable. Only the suggest field gets completion postings; every other
  field keeps the default, which is what a real caller wants and what makes the
  per-field routing worth testing."
  ^Codec []
  (let [delegate (Lucene103Codec.)
        completion (Completion101PostingsFormat.)]
    (proxy [Lucene103Codec] []
      (getPostingsFormatForField [^String field]
        (if (= suggest-field field)
          completion
          (.getPostingsFormatForField delegate field))))))

(defn- suggest-doc
  "A raw Lucene Document — `add-doc`'s field-type map has no SuggestField case,
  and `addDocument` takes any Iterable<IndexableField>, so a caller composes one
  directly."
  ^Document [^String id ^String text ^long weight]
  (doto (Document.)
    (.add (StringField. "id" id Field$Store/YES))
    (.add (SuggestField. suggest-field text (int weight)))))

(defn- add-suggest! [sw ^String id ^String text ^long weight]
  (.addDocument (sc/->writer sw) (suggest-doc id text weight)))

(defn- suggestions
  "Surface forms a prefix query returns from this reader, as a set."
  [^DirectoryReader reader ^String prefix]
  (let [searcher (SuggestIndexSearcher. reader)
        query (PrefixCompletionQuery. (StandardAnalyzer.)
                                      (Term. suggest-field prefix))]
    (into #{}
          (map (fn [^TopSuggestDocs$SuggestScoreDoc d] (str (.key d))))
          (.scoreLookupDocs (.suggest searcher query 10 false)))))

(defn- suggest-now
  "Suggestions against the branch's current committed state."
  [sw ^String prefix]
  (with-open [reader (sc/snapshot sw)]
    (suggestions reader prefix)))

;; ============================================================
;; The write path the option unblocks
;; ============================================================

(deftest suggest-fields-round-trip-with-a-codec
  (let [path (temp-dir)
        writer (sc/create-index path "main" {:codec (completion-codec)})]
    (try
      (add-suggest! writer "d1" "lucene core" 4)
      (add-suggest! writer "d2" "lucene suggest" 3)
      (add-suggest! writer "d3" "luminous" 2)
      (sc/commit! writer)

      (testing "a prefix query returns the matching surface forms"
        (is (= #{"lucene core" "lucene suggest"} (suggest-now writer "luc"))))

      (testing "a narrower prefix narrows the answer"
        (is (= #{"luminous"} (suggest-now writer "lum"))))

      (testing "a prefix nothing starts with returns nothing"
        (is (= #{} (suggest-now writer "zz"))))

      (testing "the documents are ordinary documents besides"
        (is (= 3 (sc/num-docs writer)))
        (is (= 1 (count (sc/search writer {:term [:id "d2"]})))))

      (finally
        (sc/close! writer)
        (delete-dir-recursive path)))))

(deftest suggest-fields-round-trip-with-an-iwc-fn
  (testing ":iwc-fn is the general form of the same thing"
    (let [codec (completion-codec)
          path (temp-dir)
          writer (sc/create-index
                  path "main"
                  {:iwc-fn (fn [^IndexWriterConfig config] (.setCodec config codec))})]
      (try
        (add-suggest! writer "d1" "quicksilver" 4)
        (sc/commit! writer)
        (is (= #{"quicksilver"} (suggest-now writer "quick")))
        (finally
          (sc/close! writer)
          (delete-dir-recursive path))))))

;; ============================================================
;; Branching
;; ============================================================

(deftest a-fork-suggests-from-shared-segments-and-diverges
  (let [path (temp-dir)
        codec (completion-codec)
        main (sc/create-index path "main" {:codec codec})]
    (try
      (add-suggest! main "d1" "lucene core" 4)
      (sc/commit! main)

      (let [branch (sc/fork main "feature")]
        (try
          (testing "the fork suggests what it inherited, without copying it"
            ;; Only `main` ever wrote a segment for this document; the fork's
            ;; commit point names the same segment. Completion postings are
            ;; per-segment files, so this is COW sharing, not a copy.
            (is (= #{"lucene core"} (suggest-now branch "luc"))))

          (add-suggest! branch "d2" "lucene suggest" 3)
          (sc/commit! branch)

          (testing "what the fork adds, only the fork suggests"
            (is (= #{"lucene core" "lucene suggest"} (suggest-now branch "luc")))
            (is (= #{"lucene core"} (suggest-now main "luc"))))

          (testing "and what the parent adds after the fork stays on the parent"
            (add-suggest! main "d3" "lucene monitor" 2)
            (sc/commit! main)
            (is (= #{"lucene core" "lucene monitor"} (suggest-now main "luc")))
            (is (= #{"lucene core" "lucene suggest"} (suggest-now branch "luc"))))

          (finally
            (sc/close! branch))))

      (finally
        (sc/close! main)
        (delete-dir-recursive path)))))

(deftest a-forks-own-segments-carry-completion-postings
  (testing "the codec reaches the fork's writer, not just its parent's"
    ;; The fork builds a fresh IndexWriterConfig, and a codec is not a live
    ;; setting — so a fork that did not carry the customizer would inherit its
    ;; parent's suggestions and silently write none of its own. The assertion
    ;; that catches that is a suggestion the fork alone produced, which is the
    ;; "lucene suggest" case above; here it is isolated against a fork with a
    ;; prefix its parent has nothing under.
    (let [path (temp-dir)
          main (sc/create-index path "main" {:codec (completion-codec)})]
      (try
        (add-suggest! main "d1" "alpha" 4)
        (sc/commit! main)
        (let [branch (sc/fork main "feature")]
          (try
            (add-suggest! branch "d2" "bravo" 3)
            (sc/commit! branch)
            (is (= #{"bravo"} (suggest-now branch "bra")))
            (is (= #{} (suggest-now main "bra")))
            (finally
              (sc/close! branch))))
        (finally
          (sc/close! main)
          (delete-dir-recursive path))))))

;; ============================================================
;; Time travel
;; ============================================================

(deftest suggestions-time-travel-with-the-commit-generation
  (let [path (temp-dir)
        writer (sc/create-index path "main" {:codec (completion-codec)})]
    (try
      (add-suggest! writer "d1" "alpha" 4)
      ;; `commit!` answers a bare generation unless :crypto-hash? is on.
      (let [gen-1 (sc/commit! writer)]
        (add-suggest! writer "d2" "alphabet" 3)
        (let [gen-2 (sc/commit! writer)]

          (testing "the older generation suggests only what it held"
            (with-open [reader (sc/open-reader-at writer gen-1)]
              (is (= #{"alpha"} (suggestions reader "alph")))))

          (testing "the newer generation suggests both"
            (with-open [reader (sc/open-reader-at writer gen-2)]
              (is (= #{"alpha" "alphabet"} (suggestions reader "alph")))))

          (testing "and the branch head agrees with its newest generation"
            (is (= #{"alpha" "alphabet"} (suggest-now writer "alph"))))))

      (finally
        (sc/close! writer)
        (delete-dir-recursive path)))))

;; ============================================================
;; The default path, unchanged
;; ============================================================

(deftest no-codec-behaves-exactly-as-before
  (let [path (temp-dir)
        writer (sc/create-index path "main")]
    (try
      (testing "ordinary indexing, search, commit and fork are untouched"
        (sc/add-doc writer {:title "Hello World" :body "a test document"})
        (sc/commit! writer)
        (is (= 1 (sc/num-docs writer)))
        (is (= 1 (count (sc/search writer {:term [:body "test"]}))))
        (let [branch (sc/fork writer "feature")]
          (try
            (sc/add-doc branch {:title "Second" :body "another test"})
            (sc/commit! branch)
            (is (= 2 (sc/num-docs branch)))
            (is (= 1 (sc/num-docs writer)))
            (finally
              (sc/close! branch)))))

      (testing "a SuggestField is still accepted and still not queryable"
        ;; This is the defect the option exists for, pinned rather than fixed by
        ;; default: without a completion codec Lucene writes the field through
        ;; the ordinary postings format, so the write succeeds and the read
        ;; finds no completion terms to walk.
        (add-suggest! writer "d1" "lucene core" 4)
        (sc/commit! writer)
        (is (= 2 (sc/num-docs writer)))
        (is (thrown? IllegalArgumentException (suggest-now writer "luc"))))

      (finally
        (sc/close! writer)
        (delete-dir-recursive path)))))

;; ============================================================
;; The guards on the option itself
;; ============================================================

(deftest codec-and-iwc-fn-are-exclusive
  (let [path (temp-dir)]
    (try
      (is (thrown? clojure.lang.ExceptionInfo
                   (sc/create-index path "main" {:codec (completion-codec)
                                                 :iwc-fn identity})))
      (finally
        (delete-dir-recursive path)))))

(deftest an-iwc-fn-that-replaces-the-config-is-refused
  (testing "returning a fresh IndexWriterConfig would drop branching's settings"
    ;; It would build a working writer with no branch deletion policy and no
    ;; branch-aware merge policy, so nothing fails until the branch has lost its
    ;; commit points or merged a segment another branch shares.
    (let [path (temp-dir)]
      (try
        (is (thrown-with-msg?
             java.io.IOException #"dropped a setting branching depends on"
             (sc/create-index path "main"
                              {:iwc-fn (fn [_] (IndexWriterConfig.
                                                (StandardAnalyzer.)))})))
        (finally
          (delete-dir-recursive path))))))

;; ============================================================
;; The reopen and store-backed construction sites
;; ============================================================

(deftest reopening-a-branch-suggests-when-given-the-codec-again
  ;; `open` is the construction site a fork and a create do not exercise. It is
  ;; also the one where the omission is quietest: a branch reopened without the
  ;; codec still READS its completion postings, because a segment names its own
  ;; format, and writes every new segment without them.
  (let [path (temp-dir)
        codec (completion-codec)]
    (try
      (let [main (sc/create-index path "main" {:codec codec})]
        (try
          (add-suggest! main "d1" "alpha" 4)
          (sc/commit! main)
          (sc/close! (sc/fork main "feature"))
          (finally
            (sc/close! main))))

      (let [branch (sc/open-branch path "feature" {:codec codec})]
        (try
          (testing "what the branch already held still suggests"
            (is (= #{"alpha"} (suggest-now branch "alph"))))

          (add-suggest! branch "d2" "alphabet" 3)
          (sc/commit! branch)

          (testing "and so does what the reopened writer added"
            (is (= #{"alpha" "alphabet"} (suggest-now branch "alph"))))

          (finally
            (sc/close! branch))))

      (finally
        (delete-dir-recursive path)))))

(deftest a-store-backed-index-takes-the-codec-and-its-fork-keeps-it
  ;; `createOver` is the fourth site. A store-backed fork does not go through
  ;; the Java `fork` at all — it copies a manifest and opens the new branch
  ;; afresh — so the option map is what has to carry the codec there.
  (let [root (str (temp-dir) "/store-backed")
        codec (completion-codec)
        store (store-at (str root "/store"))
        main (sc/open-store-index store (str root "/cache") "main" {:codec codec})]
    (try
      (add-suggest! main "d1" "alpha" 4)
      (sc/commit! main "seed")
      (is (sc/store-backed? main))
      (is (= #{"alpha"} (suggest-now main "alph")))

      (let [branch (sc/fork main "feature")]
        (try
          (testing "the fork inherits the parent's suggestions"
            (is (= #{"alpha"} (suggest-now branch "alph"))))

          (add-suggest! branch "d2" "bravo" 3)
          (sc/commit! branch "branch work")

          (testing "and writes its own with the codec the parent was opened with"
            (is (= #{"bravo"} (suggest-now branch "bra")))
            (is (= #{} (suggest-now main "bra"))))

          (finally
            (sc/close! branch))))

      (finally
        (sc/close! main)
        (delete-dir-recursive root)))))
