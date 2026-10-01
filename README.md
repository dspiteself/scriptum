# Scriptum

[![Clojars](https://img.shields.io/clojars/v/org.replikativ/scriptum.svg)](https://clojars.org/org.replikativ/scriptum)

Copy-on-write branching for Apache Lucene. Git-like snapshot and branch semantics on full-text search indices with structural sharing.

Built on Lucene 10.3.2. Forking a branch copies no segment data — branches share
immutable segment files — so it costs milliseconds however much is indexed.

An index can live either **on disk** as a directory tree, or **in a
[konserve](https://github.com/replikativ/konserve) store**, which is what makes
it usable on an object store. Both models support the same document, search and
branching API; they differ in how a branch is represented and collected. See
[Konserve-Backed Storage](#konserve-backed-storage).

## Core Concepts

- **Branch**: A lineage sharing base segments with trunk, with its own commit history. On disk it is a COW overlay directory; in a konserve store it is a manifest naming content-addressed blobs.
- **Snapshot**: An immutable reader at a specific commit generation. All commits are retained until explicit GC.
- **Fork**: Creates a new branch by copying segment metadata only (not data). Near-instant regardless of index size.
- **GC**: Explicit garbage collection of old snapshots, respecting branch references to shared segments.

## Cryptographic Hashing (Optional)

Scriptum supports **optional SHA-512 merkle tree hashing** for content-addressable commits and tamper detection:

- **Dual UUID System**: Each commit has both a random `commit-id` (Lucene internal) and a `content-hash` (merkle root)
- **Content-Hash**: Computed as `hash(parent-content-hash + all-segment-file-hashes)` for complete integrity verification
- **Tamper Detection**: Any file modification breaks the merkle chain
- **Parent Chaining**: Content-hashes link to parent's content-hash (not commit-id) for true merkle tree
- **Yggdrasil Integration**: Content-hash can be used as snapshot-id for distributed systems

Enable with `:crypto-hash? true` when creating an index. Metadata stored in external `scriptum-hashes/*.json` files.

## API Layers

| Layer | Namespace | Use Case |
|-------|-----------|----------|
| **Java** | `org.replikativ.scriptum.BranchIndexWriter` | Direct Java usage |
| **Core** | `scriptum.core` | Low-level Clojure wrapper, both storage models |
| **Konserve** | `scriptum.konserve` | The store-backed `Directory`, its collector and snapshots |
| **Metadata** | `scriptum.metadata` | Durable metadata index (PSS + konserve) |
| **Audit** | `scriptum.audit` | Merkle-chain verification (needs `:crypto-hash?`) |
| **Yggdrasil** | `scriptum.yggdrasil` | High-level protocols |

For Clojure users: `scriptum.yggdrasil` for high-level API, `scriptum.core` for lower-level control.

For Java users: use `BranchIndexWriter` directly.

## Getting Started

### Dependencies

Add to `deps.edn`: [![Clojars](https://img.shields.io/clojars/v/org.replikativ/scriptum.svg)](https://clojars.org/org.replikativ/scriptum)

For Maven/Gradle:
```xml
<dependency>
  <groupId>org.replikativ</groupId>
  <artifactId>scriptum</artifactId>
  <version>0.1.1</version>
</dependency>
```

### Build from Source

Java sources must be compiled before use:

```bash
clj -T:build compile-java
```

### Quick Start (Clojure)

```clojure
(require '[scriptum.core :as sc])

;; Create an index (with optional crypto-hash for content-addressable commits)
(def writer (sc/create-index "/tmp/my-index" "main" {:crypto-hash? true}))

;; Add documents (with auto-detection)
(sc/add-doc writer {:title "Hello World"
                    :id "doc-1"
                    :created (java.time.Instant/now)})

;; Commit returns detailed info when crypto-hash enabled
(sc/commit! writer "Initial commit")
;; => {:generation 4
;;     :commit-id "27e31528-909b-4bdc-a287-57ed2cec1e6a"
;;     :content-hash "0903b0d6-418c-55b3-9e6b-2c910704edeb"}

;; Verify commit integrity
(sc/verify-commit writer)
;; => {:valid? true, :commit-id "...", :errors []}

;; Search
(sc/search writer :all {:limit 10})
;; => [{:title "Hello World", :id "doc-1", :created "1771269569697", :score 1.0, :doc-id 0}]

;; Fork a branch
(def feature (sc/fork writer "experiment"))

;; Add to branch (doesn't affect main)
(sc/add-doc feature {:title "Branch only"
                     :id "doc-2"
                     :created (java.time.Instant/now)})
(sc/commit! feature "Added experimental doc")

;; Main still has 1 doc, branch has 2
(count (sc/search writer :all))    ;; => 1
(count (sc/search feature :all))   ;; => 2

;; Merge branch back
(sc/merge-from! writer feature)
(sc/commit! writer "Merged experiment")

;; Cleanup
(sc/close! feature)
(sc/close! writer)
```

## Use Case: Email Indexing

Scriptum's field types are designed for real-world use cases like email indexing:

```clojure
(require '[scriptum.core :as sc])
(import '[java.time Instant]
        '[org.apache.lucene.document LongField])

(def writer (sc/create-index "/tmp/mail-index" "main"))

;; Index an email with all metadata
(sc/add-doc writer
  {:subject "Q1 Planning Meeting"                       ; :text (analyzed, searchable)
   :body {:value "Email content..." :store? false}      ; searchable but not stored
   :from {:value "alice@example.com" :type :string}     ; exact match
   :to {:value ["bob@example.com" "charlie@example.com"]
        :type :string}                                   ; multi-valued
   :sent-date (Instant/parse "2026-02-16T10:00:00Z")    ; auto-converts to :long
   :size {:value 42000 :type :int}                      ; numeric with range queries
   :attachment-count {:value 2 :type :int}
   :message-id {:value "<abc123@example.com>" :type :stored-only}  ; retrieve only
   :headers {:value "{...raw headers...}" :type :stored-only}})

(sc/commit! writer "Indexed email batch")

;; Range query: emails from last week
(def last-week (.toEpochMilli (.minus (Instant/now) (Duration/ofDays 7))))
(def recent-emails
  (sc/search writer
    (LongField/newRangeQuery "sent-date" last-week Long/MAX_VALUE)
    {:limit 100}))

;; Full-text search in subject/body
(def results (sc/search writer {:term [:subject "planning"]} {:limit 10}))

;; Exact match by sender
(def from-alice (sc/search writer {:term [:from "alice@example.com"]} {:limit 50}))

(sc/close! writer)
```

## API Reference

### Lifecycle

```clojure
(sc/create-index path branch-name)                    ; create new index at path
(sc/create-index path branch-name {:crypto-hash? true}) ; with content-addressable commits
(sc/open-branch path branch-name)                     ; open existing branch
(sc/fork writer "branch-name")                        ; fast fork from writer
(sc/close! writer)                                    ; close writer and release resources
(sc/discover-branches path)                           ; => #{"feature"} — forks only, not main

;; Accessors
(sc/num-docs writer)                ; document count (excluding deletions; nested children count)
(sc/max-doc writer)                 ; document count (including deletions)
(sc/branch-name writer)             ; current branch name
(sc/base-path writer)               ; index base path
(sc/main-branch? writer)            ; true if this is the main branch
```

### Document Operations

Field types:
- `:text` - Analyzed, searchable full-text (default)
- `:string` - Exact match, non-analyzed (not sortable: no doc values)
- `:int`, `:long`, `:float`, `:double` - Numeric fields with range queries and [sorting](#sorting)
- `:stored-only` - Store but don't index (for retrieval-only fields)
- `:vector` - KNN float vector search with configurable similarity
- `:nested` - Vector of child maps, each indexed as its own document; see [Nested Documents](#nested-documents)

Auto-detection:
- `java.time.Instant` → `:long` (epoch millis)
- `java.util.Date` → `:long` (epoch millis)
- `float-array` → `:vector`

```clojure
;; Simple usage (auto-detection)
(sc/add-doc writer {:title "Searchable text"
                    :from "alice@example.com"
                    :date (java.time.Instant/now)})

;; Advanced usage (explicit types)
(sc/add-doc writer {:subject {:type :text :value "Meeting notes"}
                    :from    {:type :string :value "alice@example.com"}
                    :to      {:type :string :value ["bob@" "charlie@"]}  ; multi-valued
                    :date    {:type :long :value 1234567890 :store? true}
                    :size    {:type :int :value 42000 :store? false}
                    :headers {:type :stored-only :value "{...json...}"}
                    :embed   {:type :vector :value (float-array [0.1 0.2 0.3])
                              :similarity :cosine}})

;; For fine-grained control, use Lucene classes directly
(let [doc (org.apache.lucene.document.Document.)]
  (.add doc (org.apache.lucene.document.TextField. "body" text Field$Store/NO))
  (.add doc (org.apache.lucene.document.StoredField. "body" text))
  (.addDocument writer doc))

(sc/delete-docs writer "id" "doc-1")           ; delete by field+value
(sc/delete-query writer lucene-query)          ; delete by a Lucene Query
(sc/update-doc writer "id" "doc-1" new-fields) ; atomic delete+add
(sc/add-document writer lucene-doc)            ; pre-built Lucene Document
(sc/add-block writer [child ... root])         ; pre-built nested block
```

### Commit & History

```clojure
;; Commit (returns generation number by default)
(sc/commit! writer "commit message")    ; => 4

;; With crypto-hash enabled, returns detailed map
(sc/commit! writer "commit message")
;; => {:generation 4
;;     :commit-id "27e31528-..."
;;     :content-hash "0903b0d6-..."}  ; merkle root

(sc/flush! writer)                      ; flush without new commit point
(sc/merge-from! writer source-writer)   ; merge segments from another branch

(sc/list-snapshots writer)
;; => [{:generation 1 :uuid "..." :timestamp "..." :message "..." :branch "main"}
;;     {:generation 2 :uuid "..." :timestamp "..." :message "..." :branch "main"}]
```

### Cryptographic Verification

When `:crypto-hash?` is enabled, you can verify commit integrity:

```clojure
;; Verify current commit
(sc/verify-commit writer)
;; => {:valid? true
;;     :commit-id "27e31528-909b-4bdc-a287-57ed2cec1e6a"
;;     :errors []}

;; Verify specific generation
(sc/verify-commit writer {:generation 5})
;; => {:valid? false
;;     :commit-id "..."
;;     :errors ["Segment file not found: _0.cfs"]}

;; Extract content-hash from commit result
(let [{:keys [content-hash]} (sc/commit! writer "msg")]
  (println "Snapshot ID:" content-hash))  ; Use as snapshot-id in Yggdrasil
```

**Note**: Verification recomputes hashes of all segment files and compares with stored metadata. Returns `:valid? true` only if all hashes match.

### Query Builders

Scriptum provides composable query builders so you don't need to import Lucene classes directly:

```clojure
;; Full-text query on a single field (supports +, -, AND, OR, NOT, phrases, wildcards, fuzzy)
(sc/text-query :title "clojure AND functional")
(sc/text-query :body "\"copy on write\"")        ; phrase
(sc/text-query :body "lucen~")                   ; fuzzy

;; Search across multiple fields (SHOULD semantics — match in any field counts)
(sc/multi-field-query [:title :body] "clojure reactive")

;; Compose queries with boolean logic
(sc/bool-query [[(sc/text-query :title "clojure") :should]
                [(sc/text-query :body  "clojure") :should]
                [{:term [:category "programming"]}  :filter]])

;; Roots with a nested child matching a query (see Nested Documents)
(sc/nested-query :comments {:term [:comments.author "alice"]} {:score-mode :max})
;; ...and one level down: roots with a reply by bob under any comment
(sc/nested-query :comments.replies {:term [:comments.replies.author "bob"]})

;; Pass any query to search
(sc/search writer (sc/text-query :body "lucene") {:limit 10})
(sc/search writer (sc/multi-field-query [:title :body] "scriptum branching") {:limit 5})
```

`bool-query` accepts clauses as `[query occur]` pairs where `occur` is `:must`, `:should`, `:must-not`, or `:filter`. The query in each clause can be a Lucene Query object, the result of `text-query`/`multi-field-query`, or a `{:term [field value]}` map.

### Search

```clojure
;; Term query (exact match)
(sc/search writer {:term [:tag "exact-match"]} {:limit 10})

;; Match-all
(sc/search writer :all {:limit 100})

;; Full-text with query builders (no Lucene imports needed)
(sc/search writer (sc/text-query :body "clojure functional") {:limit 10})
(sc/search writer (sc/multi-field-query [:title :body] "lucene segments") {:limit 10})

;; Raw Lucene query object (e.g., range queries for numeric fields)
(import '[org.apache.lucene.document LongField])
(def last-week (.toEpochMilli (.minus (java.time.Instant/now) (java.time.Duration/ofDays 7))))
(sc/search writer (LongField/newRangeQuery "date" last-week Long/MAX_VALUE) {:limit 10})

;; Returns: [{:field1 "val" :field2 "val" :score 1.0 :doc-id 0} ...]
```

#### Sorting

Results come best score first. `:sort` orders them instead, as ES's `sort`
does: a vector of specs, each breaking the ties of the one before, with the doc
id breaking the last ones.

```clojure
;; Newest first; equal dates by score
(sc/search writer (sc/text-query :body "lucene")
           {:sort [{:field :date :type :long :order :desc} :score]})
;; => [{... :doc-id 7 :score 0.42 :sort-values [1717171717000 0.42]} ...]
```

- A spec is `:score` (best first), `:doc-id` (index order), or a map of
  `:field`, `:type`, `:order` (`:asc`, the default, or `:desc`), `:mode`,
  `:missing` and `:nested` (see [Sorting by nested
  fields](#sorting-by-nested-fields)).
- **Only numeric fields sort.** A field sort reads doc values, which scriptum
  indexes for `:int`, `:long`, `:float` and `:double` fields only, so `:type`
  is required and names one of those, as the field was indexed. `:string` and
  `:text` are refused: they have no doc values, so strings cannot be sorted
  yet. A field indexed at another width than `:type` names (an `:int` sort of
  a `:long` field) is refused when searched. `:int` and `:float` cannot be told
  apart, and neither can `:long` and `:double`.
- `:mode` picks which of a multi-valued field's values counts: `:min` (the
  default for `:asc`) or `:max` (the default for `:desc`), as in ES. ES's
  `avg`, `sum` and `median` are not supported.
- `:missing` puts hits without a value `:last` (the default) or `:first`, in
  either order.
- Each result also has `:sort-values`, its value for each spec: the score as a
  Float, the doc id as an Integer, a field's value as the Integer, Long, Float
  or Double its `:type` reads. A hit without a value shows what it sorted as,
  as ES does: the type's minimum or maximum, or -Infinity or Infinity. `:score`
  is still the query's score.
- Without `:sort` the search is exactly what it was, with no `:sort-values`.
  `search-store-snapshot` takes `:sort` too; `candidate-page` keeps its own
  `:order`.

### Nested Documents

Elasticsearch-style `nested` objects, stored as Lucene block joins. Each object
in a `:nested` field is indexed as its own child document, so the conditions of
a nested query must all hold for the same object:

```clojure
(import '[org.apache.lucene.document IntField])

(sc/add-doc writer {:id       {:value "post-1" :type :string}
                    :title    "Nested documents"
                    :comments {:type :nested
                               :value [{:author {:value "alice" :type :string}
                                        :stars  {:value 5 :type :int}}
                                       {:author {:value "bob" :type :string}
                                        :stars  {:value 1 :type :int}}]}})

;; Posts with ONE comment by alice that has 1 star. post-1 does not match;
;; flattened fields would match it, since alice and 1 star are in different comments.
(sc/search writer
  (sc/nested-query :comments
                   (sc/bool-query [[{:term [:comments.author "alice"]} :filter]
                                   [(IntField/newExactQuery "comments.stars" 1) :filter]])
                   {:score-mode :max}))
```

- A child map takes the same field specs as a document, `:nested` included
  (see below). Its fields are named `<path>.<field>` (`comments.author`), and
  child queries use those names.
- `:score-mode` sets how the matching children's scores become the root's:
  `:avg` (the default, as in ES), `:max`, `:min`, `:sum` or `:none`.
- **Only roots are results.** `search`, `:all`, `search-store-snapshot`,
  `count-store-snapshot` and `candidate-page` never return a child.
  `nested-query` matches children but returns their roots, and a hit carries
  the root's stored fields. The children that matched come back only as
  [inner hits](#inner-hits), on request.
- **Counting.** `num-docs` and `max-doc` count Lucene documents, children
  included, as ES's index stats do. To count roots, use
  `(sc/count-store-snapshot snap :all)`, or count `sc/roots-query` over any
  reader: `(.count (IndexSearcher. reader) sc/roots-query)`. `search` stops at
  its `:limit`, so `(count (sc/search writer :all))` does not count roots.
  `sc/roots-bitset` is the matching parents filter for your own block-join
  queries; share that one instance, since it caches per segment.
  `NestedQuery/parentFilter` gives the filter for the objects on one path;
  any instance of it reuses the bitsets cached for that path, until their
  segment closes.
- **Deletes and updates act on whole blocks.** Once an index holds nested
  documents, `delete-docs`, `delete-query` and `update-doc` match roots only,
  and each matched root takes its children with it, at every level. A delete
  by a field that only children carry matches nothing, and an `update-doc`
  keyed by one replaces nothing and adds the new block. To change one comment you reindex
  its post with `update-doc`, as in ES. An index that never used `:nested`
  keeps the plain term-delete path. The choice is made atomically with the
  write, so concurrent writers need no coordination while an index gets its
  first children.
- `add-block` adds a pre-built block of Lucene documents, children first and
  root last, in the order below. Each child carries one `_nested_path`
  StringField (`sc/nested-path-field`), stored or not, holding its path, and
  the root does not. Name a child's fields `<path>.<name>`, as `add-doc` does.
  A block of any other shape is refused before anything is written, as is a
  lone child passed to `add-document` and a doc-map with a top-level
  `_nested_path` key. So is a path in any other field type: stored only, it
  is not indexed and its child reads as a root; indexed any other way, it
  fixes the field's schema, and every later nested write on the branch
  throws.
- Existing indexes need no migration: a document without `_nested_path` is a
  root.
- Roots sort by their children's fields with `:nested` in a sort spec; see
  [Sorting by nested fields](#sorting-by-nested-fields).

The whole-block rule exists because a root deleted without its children
corrupts the index silently. Before a merge, nested queries still return the
deleted root through its children. At the next merge the children join the
following root's block, and Lucene's `CheckJoinIndex` does not detect it.

#### Nested inside nested

A child map may have `:nested` fields of its own, to any depth:

```clojure
(sc/add-doc writer {:id       {:value "post-2" :type :string}
                    :comments {:type :nested
                               :value [{:author  {:value "alice" :type :string}
                                        :replies {:type :nested
                                                  :value [{:author {:value "bob" :type :string}}]}}
                                       {:author  {:value "carol" :type :string}}]}})

;; Posts with a comment by alice that has a reply by bob. The inner query joins
;; to the comment the outer one is matching, so both hold for ONE comment.
(sc/search writer
  (sc/nested-query :comments
                   (sc/bool-query [[{:term [:comments.author "alice"]} :filter]
                                   [(sc/nested-query :comments.replies
                                                     {:term [:comments.replies.author "bob"]})
                                    :filter]])))

;; Posts with a reply by bob under any comment: at top level, a nested query on
;; a deeper path joins straight to the roots.
(sc/search writer (sc/nested-query :comments.replies {:term [:comments.replies.author "bob"]}))
```

- **Paths.** A path joins the `:nested` field names from the root down with
  dots: the replies above are on `comments.replies`, and their fields are
  `comments.replies.author`, as in ES. Each segment is one level, so a
  `:nested` field name may not itself contain a dot, and `add-doc` refuses one.
  Other field names may.
- **Binding.** As in ES, a nested query inside another one's child query
  joins to THAT query's objects, and a top-level one joins to the roots. The
  inner path must be under the outer one, and may skip levels: a query on
  `a.b.c` directly inside one on `a` joins each `c` to its own `a`. The inner
  query may sit in `bool-query` clauses (`:must-not` included), `BoostQuery`,
  `ConstantScoreQuery` or `DisjunctionMaxQuery`, which are rebuilt around it.
  Reached through any other query it cannot be rebound, so `nested-query`
  throws `IllegalArgumentException` rather than join it to the wrong level.
  Binding finds inner queries through `Query.visit`, though, and cannot check
  a query whose `visit` hides its subqueries, as the ones Lucene's `JoinUtil`
  builds do. A nested query hidden in one stays joined to the roots and
  matches nothing at the inner level: under `:must-not` it then excludes
  nothing, and the results are wrong, with no error. Keep nested queries out
  of such queries. Lucene's `ParentChildrenBlockJoinQuery` is one too, and
  `nested-query` refuses it inside a nested query.
- **Order.** A block is its root's tree in post-order: each object right after
  its own children, nested fields in doc-map order, the root last. That is
  what lets a block join over the objects on any one path find, for an object
  below them, its own ancestor. A pre-built block for `add-block` must keep
  every child's parent after it, before any other of its ancestors'
  documents; one that does not is refused, since its children would join
  another object, often in another root's block.

`nested-query` returns an `org.replikativ.scriptum.NestedQuery`, which keeps
its path, score mode and binding as data and becomes Lucene's
`ToParentBlockJoinQuery` when a search rewrites it. A `ToParentBlockJoinQuery`
built directly cannot be rebound, because it exposes neither its parents
filter nor its score mode.

#### Inner hits

ES's `inner_hits`: which children of each hit matched. Request them with
`:inner-hits` on a `nested-query`, and each result carries them:

```clojure
(sc/search writer
  (sc/nested-query :comments {:term [:comments.author "alice"]}
                   {:inner-hits {:size 2 :fields [:comments.stars]}}))
;; => [{"id" "post-1" "title" "Nested documents" :doc-id 2 :score 0.315
;;      :inner-hits {"comments" {:total 1
;;                               :hits [{"comments.stars" "5"
;;                                       :doc-id 0 :score 0.315
;;                                       :nested-path "comments"
;;                                       :offset 0 :path-offsets [0]}]}}}
;;     ...]
```

- `:inner-hits` is `true`, or a map of `:name` (the key in `:inner-hits`,
  default the path), `:size` (children per hit, default 3 as in ES) and
  `:fields` (the children's stored fields to return, default all).
- `:total` is the exact number of the hit's children that match the child
  query. `:hits` holds the best `:size` of them, by the child query's own
  score and then doc id, each with its stored fields under their full names.
- `:offset` is the child's position in its parent's array of objects on its
  path, ES's `_nested.offset`; objects on other paths do not count.
  `:path-offsets` gives that position for the child and every object above
  it, root first, so a reply at `[1 0]` is the first reply of the second
  comment.
- **Nesting.** A request inside another nested query that has one is
  answered under each of that query's hits, as in ES:

  ```clojure
  ;; Each comment hit carries its own :inner-hits {"comments.replies" ...}:
  ;; the replies by bob to THAT comment.
  (sc/nested-query :comments
                   (sc/bool-query [[{:term [:comments.author "alice"]} :filter]
                                   [(sc/nested-query :comments.replies
                                                     {:term [:comments.replies.author "bob"]}
                                                     {:inner-hits true})
                                    :filter]])
                   {:inner-hits true})
  ```

  Inside a nested query WITHOUT a request, it is answered at the nearest
  level up that has one, or on the result itself. Its hits are then the
  matching children under objects that match every nested query in between:
  without the outer request above, the result lists bob's replies to alice's
  comments, not his replies to anyone else. A top-level request on a deep path
  (`comments.replies`) lists every matching reply of the root.

  This differs from ES, which ignores a request inside a nested query without
  one. scriptum answers it a level up, so results carry keys ES's would not,
  and two such requests of one name that land on one level are refused where
  ES runs the query. Give each a distinct `:name`, or put `:inner-hits` on the
  enclosing query.
- A request under a `:must-not` clause is not answered, as in ES: there a hit
  matches for want of such children.
- Two requests with one name in one level are refused, as ES refuses them.
  `nested-query` checks the levels inside it as it is built, and `search` the
  whole query.
- `search` and `search-store-snapshot` return inner hits. Matching is
  unchanged, so `count-store-snapshot` and deletes ignore the request.
  `candidate-page` refuses it: candidates carry no children, so the request
  could only be dropped.

#### Sorting by nested fields

ES's nested sort. With `:nested` in a [sort spec](#sorting), each root sorts by
the field's values on its children:

```clojure
;; Posts by their best rating from alice; posts without one last
(sc/search writer :all
  {:sort [{:field  "comments.stars" :type :int :order :desc
           :nested {:path :comments
                    :filter {:term [:comments.author "alice"]}}}]})
```

- `:path` names the children and `:filter` (optional, a child-level query as in
  `nested-query`) narrows them. `:mode` takes the `:min` or `:max` of all their
  values, by default `:min` for `:asc` and `:max` for `:desc`.
- A root with no such child, whether it has no children or none that match, has
  no value and goes where `:missing` says: last by default, in either order.
- The sort joins to the roots at any depth, as in ES: a `:path` of
  `comments.replies` sorts each post by all of its replies, under every
  comment. A nested query inside `:filter` joins to the path's objects, as
  inside `nested-query`.
- `:field` must be named under `:path`, `comments.stars` for `comments`;
  `add-doc` names a child's fields so. A field of objects further down
  needs their own `:path`: with `:path` `comments`, `comments.replies.stars`
  is read on the comments, which do not have it. A dotted name could be a
  comment's own field, so that is not refused.
- Without `:nested` a sort reads the roots' own field. A field that only
  children carry has no value on any root, so every root would sort as missing.
- Not supported: ES's per-level filters (`nested.nested`), `max_children` and
  a numeric `missing` value, which are refused (`:nested` inside `:nested`,
  `:max-children`, `:missing 0`), and sorting by ascending score. A deep
  `:path` without per-level filters reads all of its objects under each root.
- A sort without `:filter` shares one cached bitset of the path's children per
  segment. A sort with a `:filter` computes the filter's bitset per segment on
  each search, and keeps nothing once the search returns.

### Time Travel

```clojure
;; Get snapshot at specific generation
(def reader (sc/open-reader-at writer 1))

;; Check if a generation still exists (may be GC'd)
(sc/commit-available? writer 1)  ; => true/false

;; Get current immutable snapshot
(def snap (sc/snapshot writer))

;; Execute with auto-closing snapshot
(sc/with-snapshot writer
  (fn [reader]
    ;; Search on immutable snapshot
    (let [searcher (org.apache.lucene.search.IndexSearcher. reader)]
      (.search searcher (org.apache.lucene.search.MatchAllDocsQuery.) 10))))

(.close reader)
```

### Garbage Collection

```clojure
;; Remove commits older than an hour, respecting branch references
(sc/gc! writer (.minus (java.time.Instant/now) (java.time.Duration/ofHours 1)))
```

GC only runs on the main branch and protects all segment files referenced by any
branch.

**It reclaims nothing while a branch still shares files with main** — which
after an ordinary fork is always. Protection is per commit point (one is spared
if it references any file a branch references) and a fresh fork shares every
base segment, so every commit point on main stays pinned. Measured over 6
commits: 6 removed with no branch, **0** with one untouched fork, 7 once that
fork has merged away its inherited segments. A call that reclaims nothing still
adds a commit point.

The conservatism is in the safe direction. The **store-backed** model avoids the
sharing problem — reachability is computed across every branch's manifest — but
still needs `retain!` before anything becomes unreachable; see Retention below.

### Store-backed operations

These exist only for an index in a konserve store, and are covered in
[Konserve-Backed Storage](#konserve-backed-storage):

```clojure
(sc/open-store-index store cache branch)      ; open a branch in a store
(sc/open-store-index-at store cache branch a) ; open it at a specific state
(sc/snapshot-address writer)                  ; the immutable address of that state
(sc/warm! writer)                             ; fetch a cold index in parallel
(sc/retain! writer {:before instant})         ; drop old commit points
(sk/gc! store store-id)                       ; collect the store
(sk/gc-cache! store cache)                    ; collect the local cache
(sk/mark store)                               ; the root set, for an embedding collector
```

`scriptum.core/gc!` is for directory-backed indices and throws on a store-backed
one; `scriptum.konserve/gc!` is its counterpart.

## Java API

For Java users, `BranchIndexWriter` provides the complete API:

```java
import org.replikativ.scriptum.BranchIndexWriter;
import org.apache.lucene.document.*;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

// Create an index (with optional crypto-hash)
BranchIndexWriter main = BranchIndexWriter.create(
    Path.of("/tmp/my-index"),
    "main",
    new StandardAnalyzer(),
    true  // enable crypto-hash for content-addressable commits
);

// Add documents
Document doc = new Document();
doc.add(new TextField("title", "Hello World", Field.Store.YES));
doc.add(new StringField("id", "doc-1", Field.Store.YES));
main.addDocument(doc);
main.commit("Initial commit");

// Get content-hash and commit-id
String contentHash = main.getLastContentHash();  // Merkle root (content-addressable)
String commitId = main.getLastCommitId();        // Lucene's random UUID

// Verify commit integrity
Map<String, Object> result = main.verifyCommit(-1);  // -1 = current HEAD
boolean valid = (Boolean) result.get("valid");
if (!valid) {
    List<String> errors = (List<String>) result.get("errors");
    System.err.println("Integrity check failed: " + errors);
}

// Fork a branch (few ms regardless of index size)
BranchIndexWriter feature = main.fork("experiment");
feature.addDocument(anotherDoc);
feature.commit("Feature work");

// Search
DirectoryReader reader = main.openReader();
IndexSearcher searcher = new IndexSearcher(reader);
TopDocs results = searcher.search(new MatchAllDocsQuery(), 10);
reader.close();

// Merge branch back
main.mergeFrom(feature);

// Time travel - open reader at specific generation
DirectoryReader historical = main.openReaderAt(1);

// Garbage collect old commits
main.gc(Instant.now().minus(Duration.ofHours(1)));

// Discover branches
Set<String> branches = BranchIndexWriter.discoverBranches(Path.of("/tmp/my-index"));

// Cleanup
feature.close();
main.close();
```

### Key Java Methods

| Method | Description |
|--------|-------------|
| `create(path, branchName, analyzer, cryptoHash)` | Create new index with optional crypto-hash |
| `open(path, branchName)` | Open existing branch |
| `fork(branchName)` | Fast fork (copies metadata only) |
| `addDocument(doc)` | Add a document |
| `addDocuments(docs)` | Add a block atomically (nested: children first, parent last) |
| `deleteDocuments(terms...)` | Delete by terms |
| `updateDocument(term, doc)` | Atomic delete+add |
| `updateDocuments(term or query, docs)` | Atomic delete+add of a block |
| `mayHoldNested()` | Whether the branch may hold nested children (set once a block arrives) |
| `commit()` / `commit(message)` | Persist changes |
| `getLastCommitId()` | Get last commit's UUID (Lucene internal) |
| `getLastContentHash()` | Get last commit's merkle root (if crypto-hash enabled) |
| `verifyCommit(generation)` | Verify commit integrity (returns Map with "valid", "errors") |
| `openReader()` | NRT reader (sees uncommitted) |
| `openCommittedReader()` | Reader on committed state |
| `openReaderAt(generation)` | Time travel to specific commit |
| `isCommitAvailable(generation)` | Check if commit still exists |
| `listSnapshots()` | Get all commit points |
| `mergeFrom(source)` | Merge another branch |
| `gc(beforeInstant)` | Garbage collect old commits |
| `numDocs()` / `maxDoc()` | Document counts |
| `getBranchName()` | Current branch name |
| `isMainBranch()` | Check if main branch |

A nested block must be deleted whole. `deleteDocuments(term)` or
`updateDocument(term, doc)` on a parent's id removes the parent and leaves its
children live, and after the next merge they belong to the following parent.
To delete a parent from Java, pass `deleteDocuments(query)` the parent query OR
a `ToChildBlockJoinQuery` over it, which is what `delete-docs` does in Clojure.

From Java, `new NestedQuery("comments", childQuery, ScoreMode.Avg)` is the
nested query, joined to the roots; nested queries inside `childQuery` are bound
to `comments` as it is built. `NestedQuery.rootsQuery()`,
`NestedQuery.rootsFilter()` and `NestedQuery.parentFilter(path)` are the root
query and the parents filters, which share their per-segment bitsets across
every use. To sort roots by a nested field, give Lucene's
`ToParentBlockJoinSortField` `rootsFilter()` as its parents filter and
`parentFilter(path)` as its child filter. Its fourth argument, `order`, is
not a direction: `true` takes the children's maximum and `false` their
minimum. Set a missing value too, or roots without children sort as 0.

## Konserve-Backed Storage

An index can live in a [konserve](https://github.com/replikativ/konserve) store
instead of a directory tree, which is what makes it usable on an object store.
Konserve is the source of truth; the local directory is a derived cache that may
be deleted at any time and rebuilt from the store.

A branch is a manifest rather than a directory:

```
[:scriptum :manifest <branch>]  ->  <commit address>          ; the one mutable cell
[:scriptum :snapshot <address>] ->  {:files   {name -> address}
                                     :parents [<address> ...]}
[:scriptum :blob <address>]     ->  segment bytes             ; content-addressed
```

Segments shared between branches are one blob, and one inode locally, so a fork
copies a pointer and no bytes move.

```clojure
(require '[konserve.store :as kstore]
         '[scriptum.core :as sc]
         '[scriptum.konserve :as sk])

;; The store must carry a konserve :id — connect-store attaches one,
;; konserve.filestore/connect-fs-store does not, and scriptum refuses a store
;; without one because the GC guard is keyed on it.
(def store (kstore/create-store {:backend :file
                                 :path "/data/index-store"
                                 :id #uuid "..."}
                                {:sync? true}))

(def writer (sc/open-store-index store "/tmp/index-cache" "main"))
(sc/add-doc writer {:title "Hello" :body "world"})
(sc/commit! writer "first")

(sc/fork writer "feature")        ; copies a pointer; no bytes move
(sc/branches writer)              ; => #{"main" "feature"}
```

### Snapshots are values

Every commit has an immutable, content-addressed address covering its files and
its ancestry. Hold one and you can come back to exactly that state:

```clojure
(def held (sc/snapshot-address writer))     ; a UUID naming this index state

;; read-only, on any machine with the store
(with-open [d (sk/snapshot-directory store "/tmp/cache" held)]
  ...)

;; or restore a branch to it, writable
(sc/open-store-index-at store "/tmp/cache" "main" held)
(sk/fork-from-snapshot! store "from-held" held)
```

### Warming a cold machine

Materialization is lazy — a selective query does not pay for segments it never
reads. On a machine with an empty cache that is about to serve, that is the
wrong trade: Lucene opens segment readers serially, so a cold query costs one
round trip per file, in sequence. Measured on a 35-segment index at 60 ms
latency: **2.2 s lazily against 275 ms warmed**.

```clojure
(sc/warm! writer)                                     ; fetch this branch, in parallel
(sc/warm! writer {:only #(str/ends-with? % ".cfs")})  ; or part of it
```

Lucene's own warming hooks do not help here, because they all assume the file is
already on the machine: `IndexInput.prefetch` fires after this Directory has
materialized the whole blob, `MMapDirectory.setPreload` pages in what is on
disk, and `setMergedSegmentWarmer` warms this writer's own merges.

### Collection

`scriptum.core/gc!` is for directory-backed indices and throws here. A
store-backed index collects by reachability:

```clojure
(sk/gc! store (sk/store-id-for store))       ; collect the store
(sk/gc-cache! store "/tmp/index-cache")      ; and the local cache, separately
```

Both take the snapshot addresses an external holder still references, so a state
no branch names is not collected out from under them.

**On a store you do not own** — one shared with datahike, say — do not call
`sk/gc!` at all. konserve's sweep is allow-list, so it would delete every key
scriptum does not name. Use `sk/mark` to contribute scriptum's keys to that
store's own collector, unioned with `scriptum.metadata/mark` if a metadata index
shares the store.

### Retention

Every commit point is kept by default, so a branch's file map is cumulative —
30 commits of 30 documents were measured naming 130 files, 30 of them commit
points. All of that is legitimately reachable, which is why collection reclaims
nothing however long the index runs. `retain!` is what bounds it:

```clojure
(sc/retain! writer {:before (.minus (Instant/now) (Duration/ofDays 30))})
(sk/gc! store (sk/store-id-for store))     ; now there is something to collect
```

Dropping a commit point removes its files from the manifest; the collector then
takes the blobs no other branch names. Reading a dropped commit **by generation**
stops working — its state is still reachable by snapshot address.

**Holding an address pins nothing.** `snapshot-address` hands you a value; it
registers no claim. To keep a state alive you must pass it on every collection,
to both collectors:

```clojure
(def held (sc/snapshot-address writer))
(sk/gc! store (sk/store-id-for store) (ku/now) #{held})
(sk/gc-cache! store "/tmp/index-cache" #{held})
```

Under yggdrasil this is automatic: the coordinator computes reachability from
every system's `gc-roots` and the commit graph, and `gc-sweep!` drops the
candidates it is handed.

### Limits worth knowing

- **One writer per branch**, in one JVM. The write lock lives in the local
  cache, so it does not span machines, and the manifest write is not yet a
  compare-and-set. Writers on *different* branches are safe by construction.
- **History accumulates until you prune it.** Every commit point is retained by
  default, so the store grows with commit count regardless of live document
  count. See Retention above.
- **`scriptum.audit` needs `:crypto-hash?`**, which store-backed indices do not
  yet enable, so audit degrades to `{:status :advisory}` there.
- Against a remote store, start from `scriptum.konserve/remote-tuning` — Lucene's
  default 5 GB merged-segment cap is not appropriate for an object store.

## Yggdrasil Integration

Scriptum implements the [Yggdrasil](https://github.com/replikativ/yggdrasil) protocol stack (Snapshotable, Branchable, Graphable, Mergeable):

```clojure
(require '[scriptum.yggdrasil :as sy]
         '[yggdrasil.protocols :as p])

(def sys (sy/create "/tmp/my-index" {:system-name "search-index"}))

(p/branches sys)         ; => #{:main}
(p/branch! sys :feature)
(p/checkout sys :feature)
;; ... add docs, commit ...
(p/merge! sys :main)
(p/history sys {:limit 10})

(sy/close! sys)
```

Passes the full yggdrasil compliance test suite (22 tests, 203 assertions).

## Performance

Typical results:
- **Fork**: no segment data is copied, so it is independent of how much is
  indexed — but it scans the index directory to pick a free segment ordinal, so
  it grows with the number of *files*. Measured 8 ms at 548 files, 28 ms at 2235.
- **Indexing**: ~50k docs/sec (text fields, SSD)
- **Search**: sub-millisecond for simple queries

Store-backed, measured against a 60 ms-per-request store:
- **Commit**: 6 requests — 4 segment blobs, 1 snapshot, 1 branch pointer. Blobs
  upload in parallel: a 35-segment commit takes 348 ms against 2100 ms serially.
- **Cold read**: Lucene opens segment readers serially, so a cold query costs one
  round trip per file. `warm!` fetches them in parallel first — 2.2 s against
  275 ms on a 35-segment index.

## Directory Layout

On disk, scriptum uses this structure:

```
basePath/                    -- trunk (main branch)
  _0.cfs, _1.cfs, ...       -- shared segment files
  segments_N                 -- main's commit points
  scriptum-metadata/         -- durable metadata index (konserve store)
  scriptum-hashes/           -- crypto-hash metadata (if enabled)
    <commit-uuid>.json       -- merkle tree data per commit
  branches/
    feature/                 -- branch overlay
      _10000.cfs, ...        -- branch-specific segments
      segments_N             -- branch's commit points
```

Branches share base segments via read-only references. Only new writes create branch-specific segment files.

When crypto-hash is enabled, each commit generates a JSON metadata file containing:
- `content-hash`: The merkle root (content-addressable commit ID)
- `parent-content-hash`: Parent commit's content-hash (for merkle chain)
- `segments`: Map of segment names to file hashes (SHA-512 of each segment file)

## Technical Documentation

See [docs/LUCENE_EXTENSION.md](docs/LUCENE_EXTENSION.md) for a deep-dive into how Scriptum extends Lucene:

- How Lucene segments and commit points work
- BranchedDirectory: overlay pattern for COW reads/writes
- BranchDeletionPolicy: retaining all commits until explicit GC
- BranchAwareMergePolicy: preventing merge of shared segments
- Fork operation mechanics and performance analysis
- GC with branch protection

## Project Structure

```
src/
  clojure/scriptum/
    core.clj                 # Low-level COW branching API, both storage models
    konserve.clj             # Store-backed Directory, collector, snapshots
    metadata.clj             # Durable metadata index (PSS + konserve)
    audit.clj                # Merkle-chain verification
    yggdrasil.clj            # Yggdrasil protocol adapter
  java/org/replikativ/scriptum/
    BranchIndexWriter.java   # Branch-aware Lucene writer (main Java API)
    BranchedDirectory.java   # COW directory overlay
    BranchAwareMergePolicy.java  # Prevents merging shared segments
    BranchDeletionPolicy.java    # Retains all commits until GC
    ContentHash.java         # SHA-512 hashing for merkle trees
docs/
  LUCENE_EXTENSION.md        # Technical deep-dive
test/scriptum/
  core_test.clj              # Unit tests
  konserve_test.clj          # Store-backed storage model
  crypto_test.clj            # Crypto-hash integrity tests
  audit_test.clj             # Merkle-chain verification
  yggdrasil_test.clj         # Compliance tests
  tck.clj, tck_runner.clj    # Lucene's own Directory conformance suite
```

Both directories are held to Lucene's `BaseDirectoryTestCase`, which is what
`MMapDirectory` itself is tested with:

```bash
clj -T:build compile-tck && clj -M:local:tck -m scriptum.tck-runner
```

## Requirements

- Java 21+
- Clojure 1.12.0+
- Apache Lucene 10.3.2 (pulled from Maven Central)

## Development

```bash
# Compile Java sources
clj -T:build compile-java

# Run tests
clj -T:build compile-java && clj -M:test

# Start nREPL
clj -T:build compile-java && clj -M:repl
```

## License

Copyright (c) 2026 Christian Weilbach

Licensed under the Apache License, Version 2.0.
