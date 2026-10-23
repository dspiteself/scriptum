(ns scriptum.manifest-fence-test
  "Pins the manifest fence: two writers on ONE branch, which the local
  `write.lock` cannot see because each has its own cache — two machines, as far
  as Lucene is concerned. The filestore's conditional writes reach `:machine`,
  which is enough to stand in for a remote store's `:global` here.

  Through the public API (`open-store-index`, `add-doc`, `commit!`), because that
  is what embedders call. What a fenced IndexWriter does afterwards is Lucene's
  business, so nothing here asserts it stays usable; fenced writers are only
  aborted."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.java.io :as io]
            [konserve.core :as k]
            [konserve.gc-guard :as guard]
            [konserve.store :as kstore]
            [scriptum.core :as sc]
            [scriptum.konserve :as sk])
  (:import [org.apache.lucene.index DirectoryReader]
           [org.apache.lucene.search IndexSearcher]))

(def ^:dynamic *root* nil)

(defn- rm-rf [^java.io.File f]
  (when (.isDirectory f) (run! rm-rf (.listFiles f)))
  (.delete f))

(use-fixtures :each
  (fn [t]
    (let [root (str "/tmp/scriptum-manifest-fence-test/" (random-uuid))]
      (try (binding [*root* root] (t))
           (finally (rm-rf (io/file root)))))))

(defn- store
  "One filestore per test, connected through `connect-store` so it carries the
  `:id` the GC guard keys on. Every writer below shares it."
  []
  (kstore/create-store {:backend :file :path (str *root* "/store") :id (random-uuid)}
                       {:sync? true}))

(defn- cache [machine] (str *root* "/cache-" machine))

(defn- ids-at
  "Every stored :id in the index state at `address`, as a set."
  [s address]
  (with-open [d (sk/snapshot-directory s (cache "reader") address)
              r (DirectoryReader/open d)]
    (let [sf (.storedFields (IndexSearcher. r))]
      (into #{} (map #(.get (.document sf %) "id")) (range (.numDocs r))))))

(defn- head-ids [s branch] (ids-at s (sk/branch-snapshot s branch)))

(defn- commit-doc! [w id]
  (sc/add-doc w {:id id})
  (sc/commit! w id))

(defn- refused
  "The throwable `f` raised, or nil if it returned."
  [f]
  (try (f) nil (catch Throwable t t)))

(defn- abort-quietly! [w]
  (try (sc/abort! w) (catch Throwable _ nil)))

(deftest the-filestore-can-fence
  (is (k/conditional-write? (store) :machine)
      "every other test here relies on it"))

(deftest a-second-writer-is-refused-rather-than-silently-replacing-the-first
  (let [s (store)]
    (let [w (sc/open-store-index s (cache "a") "main")]
      (commit-doc! w "seed")
      (sc/close! w))
    (let [a (sc/open-store-index s (cache "a") "main")
          b (sc/open-store-index s (cache "b") "main")]
      (try
        (commit-doc! a "a1")
        (let [after-a (sk/branch-snapshot s "main")
              t (refused #(commit-doc! b "b1"))]
          (testing "the second writer's commit fails, and says why"
            (is (some? t))
            (is (sk/branch-moved? t)))
          (testing "the branch still names the first writer's commit"
            (is (= after-a (sk/branch-snapshot s "main")))
            (is (= #{"seed" "a1"} (head-ids s "main")))))
        (testing "the first writer carries on"
          (commit-doc! a "a2")
          (is (= #{"seed" "a1" "a2"} (head-ids s "main"))))
        (testing "the refused writer stays fenced"
          (is (sk/branch-moved? (refused #(commit-doc! b "b2"))))
          (is (= #{"seed" "a1" "a2"} (head-ids s "main"))))
        (finally
          (abort-quietly! b)
          (sc/close! a))))
    (testing "reopened from the head, the refused machine commits normally"
      (let [b (sc/open-store-index s (cache "b") "main")]
        (try
          (commit-doc! b "b3")
          (is (= #{"seed" "a1" "a2" "b3"} (head-ids s "main")))
          (finally (sc/close! b)))))))

(deftest a-fenced-writer-releases-the-gc-guard
  (testing "what the refused commit uploaded can never be published, so it must
            not hold collection off for as long as the writer stays open"
    (let [s (store)
          id (sk/store-id-for s)
          a (sc/open-store-index s (cache "a") "main")
          b (sc/open-store-index s (cache "b") "main")]
      (try
        (commit-doc! a "a1")
        (is (sk/branch-moved? (refused #(commit-doc! b "b1"))))
        (is (not (guard/in-flight? id)))
        (finally
          (abort-quietly! b)
          (sc/close! a))))))

(deftest point-branch-at-fences-a-live-writer
  (testing "a branch reset under a live writer is not undone by its next commit"
    (let [s (store)
          w (sc/open-store-index s (cache "a") "main")]
      (try
        (commit-doc! w "one")
        (let [one (sk/branch-snapshot s "main")]
          (commit-doc! w "two")
          (sk/point-branch-at! s "main" one)
          (is (sk/branch-moved? (refused #(commit-doc! w "three"))))
          (is (= one (sk/branch-snapshot s "main")))
          (is (= #{"one"} (head-ids s "main"))))
        (finally (abort-quietly! w))))))

(deftest two-writers-creating-one-branch
  (testing "a branch with no manifest yet is fenced on its absence, so only one
            first commit lands"
    (let [s (store)
          a (sc/open-store-index s (cache "a") "fresh")
          b (sc/open-store-index s (cache "b") "fresh")]
      (try
        (commit-doc! a "a1")
        (is (sk/branch-moved? (refused #(commit-doc! b "b1"))))
        (is (= #{"a1"} (head-ids s "fresh")))
        (finally
          (abort-quietly! b)
          (sc/close! a))))))

(deftest a-deleted-branch-is-recreated-only-if-nobody-else-has
  (testing "a delete under a live writer is not a move — its next commit
            recreates the branch, as before the fence (konserve-test pins that)
            — but a branch another writer recreated first fences it"
    (let [s (store)]
      (let [w (sc/open-store-index s (cache "a") "main")]
        (commit-doc! w "seed")
        (sc/close! w))
      (let [a (sc/open-store-index s (cache "a") "main")]
        (try
          (sk/delete-branch! s "main")
          (let [b (sc/open-store-index s (cache "b") "main")]
            (try
              (commit-doc! b "b1")
              (finally (sc/close! b))))
          (is (sk/branch-moved? (refused #(commit-doc! a "a1"))))
          (is (= #{"b1"} (head-ids s "main")))
          (finally (abort-quietly! a)))))))

(deftest a-manifest-without-a-revision-is-fenced-from-its-first-flip-on
  (testing "a manifest written before konserve stamped revisions has none to
            expect, so that writer's first flip is unconditional — and the
            revision it produces fences everything after"
    (let [s (store)]
      (let [w (sc/open-store-index s (cache "a") "main")]
        (commit-doc! w "seed")
        (sc/close! w))
      (let [legacy-head sk/branch-head
            a (with-redefs [sk/branch-head (fn [s b] [(first (legacy-head s b)) nil])]
                (sc/open-store-index s (cache "a") "main"))
            b (sc/open-store-index s (cache "b") "main")]
        (try
          (commit-doc! a "a1")
          (is (= #{"seed" "a1"} (head-ids s "main"))
              "the unconditional first flip lands")
          (is (sk/branch-moved? (refused #(commit-doc! b "b1")))
              "and moved the branch under a writer that had a revision")
          (commit-doc! a "a2")
          (is (= #{"seed" "a1" "a2"} (head-ids s "main"))
              "after it, the legacy writer is fenced on its own revision")
          (finally
            (abort-quietly! b)
            (sc/close! a)))))))

(deftest a-store-without-conditional-writes-is-unchanged
  (testing "no revisions to fence on, so the pointer is written as it always was
            — last writer wins, which is why such a store keeps to one writer per
            branch in one JVM"
    (let [s (store)]
      (with-redefs [k/conditional-write? (constantly false)]
        (let [a (sc/open-store-index s (cache "a") "main")
              b (sc/open-store-index s (cache "b") "main")]
          (try
            (commit-doc! a "a1")
            (commit-doc! b "b1")
            (is (= #{"b1"} (head-ids s "main")))
            (finally
              (sc/close! b)
              (sc/close! a))))))))

(deftest branch-moved?-reads-the-cause-chain
  (let [moved (ex-info "moved" {:type :scriptum/branch-moved})]
    (is (sk/branch-moved? moved))
    (is (sk/branch-moved? (java.io.IOException. "commit failed" moved)))
    (is (not (sk/branch-moved? (ex-info "other" {:type :something-else}))))
    (is (not (sk/branch-moved? (RuntimeException. "plain"))))))
