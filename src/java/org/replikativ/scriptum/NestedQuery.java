package org.replikativ.scriptum;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToChildBlockJoinQuery;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;

/**
 * Elasticsearch's {@code nested} query over scriptum's block joins: matches the documents on
 * {@link #getParentPath()} (the roots when it is null) that have a nested child on {@link
 * #getPath()} matching {@link #getChildQuery()}. scriptum.core's {@code nested-query} builds it.
 *
 * <p>PATHS. A path is dot-joined field names, one per nesting level: a {@code replies} field inside
 * a {@code comments} object is {@code "comments.replies"}. Every child document carries its path
 * in {@link BranchIndexWriter#NESTED_PATH_FIELD}, and roots carry nothing.
 *
 * <p>WHY NOT A {@link ToParentBlockJoinQuery} DIRECTLY. Which documents are the parents depends on
 * where the query sits. As in ES, a nested query joins to its nearest ENCLOSING nested query, or
 * to the roots at top level, so an inner query's parents are known only once the query around it
 * is built. A ToParentBlockJoinQuery exposes neither its parents filter nor its score mode, so it
 * cannot be rebound then. This class keeps both as data, is bound when an enclosing NestedQuery is
 * constructed, and becomes a ToParentBlockJoinQuery only in {@link #rewrite}, which every search,
 * count, bitset and delete runs before it asks for a Weight.
 *
 * <p>WHY THE JOIN IS RIGHT AT EVERY LEVEL. A block is its root's tree in post-order: each nested
 * object's own children come before it, and the root comes last. A block join maps a matching
 * child to the next parent after it. With parents = every document on path P, the next one after a
 * document under P is that document's own P ancestor: post-order writes the ancestor last in its
 * own subtree, and nothing else in that subtree is on P, since everything below it has a longer
 * path. This holds for any ancestor, not only the parent, so a query on {@code "a.b.c"} inside one
 * on {@code "a"} joins each c to its own a, and a top-level query on {@code "comments.replies"}
 * joins each reply straight to its root.
 *
 * <p>BINDING NEVER GUESSES. Binding walks the query types whose clauses keep their meaning when
 * rebuilt ({@code BooleanQuery}, {@code BoostQuery}, {@code ConstantScoreQuery} and {@code
 * DisjunctionMaxQuery}), and refuses a nested query it finds through any other type rather than
 * leave it joined to the wrong level. One it cannot see at all, inside a query whose {@code visit}
 * hides its subqueries, keeps its own binding; a nested query left joined to the roots then
 * matches only roots, which the enclosing level's path filter ({@link #childLevelQuery()})
 * excludes, so it finds nothing rather than the wrong documents.
 *
 * <p>INNER HITS. A nested query may carry an inner-hits request ({@link #getInnerHits()}), which
 * changes nothing about what it matches. {@link #innerHitsOf(Query)} finds the requests in a
 * query, arranged as their hits nest, and gives each the query its hits must match; scriptum.core's
 * {@code search} runs those under each hit.
 */
public final class NestedQuery extends Query {

  private static final String PATH_FIELD = BranchIndexWriter.NESTED_PATH_FIELD;

  /**
   * Every document that is not a nested child. {@code PrefixQuery} on the empty prefix matches any
   * indexed value; {@code FieldExistsQuery} cannot stand in, since it reads doc values or norms, a
   * StringField has neither, and it throws.
   */
  private static final Query ROOTS_QUERY =
      new BooleanQuery.Builder()
          .add(new MatchAllDocsQuery(), BooleanClause.Occur.FILTER)
          .add(new PrefixQuery(new Term(PATH_FIELD, "")), BooleanClause.Occur.MUST_NOT)
          .build();

  private static final QueryBitSetProducer ROOTS_FILTER = new QueryBitSetProducer(ROOTS_QUERY);

  /**
   * One parents filter per path, SHARED, because {@code QueryBitSetProducer} caches its bitset per
   * segment core: a fresh producer per query would recompute it on every search. Paths come from a
   * schema, so this stays as small as the set of nested fields.
   */
  private static final ConcurrentMap<String, QueryBitSetProducer> PATH_FILTERS =
      new ConcurrentHashMap<>();

  private final String path;
  private final Query childQuery;
  private final ScoreMode scoreMode;
  private final String parentPath;
  private final Object innerHits;
  private final Query childLevelQuery;

  /**
   * A nested query joined to the roots, as at top level. Nested queries in {@code childQuery} are
   * bound to {@code path}; see {@link #NestedQuery(String, Query, ScoreMode, String, Object)}.
   */
  public NestedQuery(String path, Query childQuery, ScoreMode scoreMode) {
    this(path, childQuery, scoreMode, null, null);
  }

  /**
   * A nested query joined to the documents on {@code parentPath}, or to the roots when it is null.
   *
   * <p>Every nested query {@code childQuery} contains directly (through boolean, boost,
   * constant-score and dis-max queries, not through another nested query) is rebound to {@code
   * path}, and its own path must be under {@code path}: {@code "a.b"} is under {@code "a"}, {@code
   * "ab"} is not. An enclosing NestedQuery overrides {@code parentPath} in the same way.
   *
   * @param path the children's path; dot-joined, non-empty field names
   * @param childQuery what a child must match, over its full field names ({@code
   *     "comments.author"})
   * @param scoreMode how matching children's scores become the parent's
   * @param parentPath the parents' path, which {@code path} must be under; null for the roots
   * @param innerHits null for none; otherwise an inner-hits request, which scriptum.core reads
   *     (true or an options map). Opaque to this class: carried through binding, compared by equals
   * @throws IllegalArgumentException if a path is malformed, {@code parentPath} is not above {@code
   *     path}, or a contained nested query is not under {@code path} or cannot be bound
   */
  public NestedQuery(
      String path, Query childQuery, ScoreMode scoreMode, String parentPath, Object innerHits) {
    this(
        checkPath(path),
        bindChildren(path, Objects.requireNonNull(childQuery, "childQuery")),
        Objects.requireNonNull(scoreMode, "scoreMode"),
        checkParent(path, parentPath),
        innerHits,
        null);
  }

  /** For a {@code childQuery} already bound to {@code path}; validates nothing. */
  private NestedQuery(
      String path,
      Query childQuery,
      ScoreMode scoreMode,
      String parentPath,
      Object innerHits,
      Void bound) {
    this.path = path;
    this.childQuery = childQuery;
    this.scoreMode = scoreMode;
    this.parentPath = parentPath;
    this.innerHits = innerHits;
    // ToParentBlockJoinQuery requires that its child query never match a parent. The path filter
    // guarantees it whatever childQuery is, and keeps a query on one path off another's children.
    this.childLevelQuery =
        new BooleanQuery.Builder()
            .add(childQuery, BooleanClause.Occur.MUST)
            .add(new TermQuery(new Term(PATH_FIELD, path)), BooleanClause.Occur.FILTER)
            .build();
  }

  /**
   * Matches every document that is not a nested child, i.e. every root. scriptum.core's {@code
   * roots-query}.
   */
  public static Query rootsQuery() {
    return ROOTS_QUERY;
  }

  /**
   * The parents filter for the roots: one shared instance. scriptum.core's {@code roots-bitset}.
   */
  public static QueryBitSetProducer rootsFilter() {
    return ROOTS_FILTER;
  }

  /**
   * The parents filter for the documents on {@code parentPath}, or {@link #rootsFilter()} when it
   * is null. One shared instance per path, so per-segment bitsets are computed once.
   *
   * @throws IllegalArgumentException if {@code parentPath} is malformed
   */
  public static BitSetProducer parentFilter(String parentPath) {
    if (parentPath == null) {
      return ROOTS_FILTER;
    }
    return PATH_FILTERS.computeIfAbsent(
        checkPath(parentPath),
        p -> new QueryBitSetProducer(new TermQuery(new Term(PATH_FIELD, p))));
  }

  /** Whether {@code path} is one or more non-empty field names joined by dots. */
  public static boolean isValidPath(String path) {
    if (path == null || path.isEmpty()) {
      return false;
    }
    for (String segment : path.split("\\.", -1)) {
      if (segment.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  private static String checkPath(String path) {
    if (!isValidPath(path)) {
      throw new IllegalArgumentException(
          "nested path must be non-empty field names joined by '.', got: \"" + path + "\"");
    }
    return path;
  }

  private static String checkParent(String path, String parentPath) {
    if (parentPath != null && !(isValidPath(parentPath) && isUnder(path, parentPath))) {
      throw new IllegalArgumentException(
          "nested path \"" + path + "\" is not under parent path \"" + parentPath + "\"");
    }
    return parentPath;
  }

  /** Whether {@code path} is strictly below {@code ancestor}: "a.b" is under "a", "ab" is not. */
  private static boolean isUnder(String path, String ancestor) {
    return path.length() > ancestor.length() + 1
        && path.startsWith(ancestor)
        && path.charAt(ancestor.length()) == '.';
  }

  /**
   * {@code query} with every nested query it directly contains joined to {@code path}.
   *
   * <p>Rebuilds only what changes and returns {@code query} itself when nothing does. Does not
   * descend into a nested query: its own children were bound to its path when it was built, and
   * rebinding it changes only its parent path.
   */
  private static Query bindChildren(String path, Query query) {
    if (query instanceof NestedQuery nested) {
      return nested.boundTo(path);
    }
    if (query instanceof BooleanQuery bool) {
      BooleanQuery.Builder builder =
          new BooleanQuery.Builder()
              .setMinimumNumberShouldMatch(bool.getMinimumNumberShouldMatch());
      boolean changed = false;
      for (BooleanClause clause : bool.clauses()) {
        Query bound = bindChildren(path, clause.query());
        changed |= bound != clause.query();
        builder.add(bound, clause.occur());
      }
      return changed ? builder.build() : bool;
    }
    if (query instanceof BoostQuery boost) {
      Query bound = bindChildren(path, boost.getQuery());
      return bound == boost.getQuery() ? boost : new BoostQuery(bound, boost.getBoost());
    }
    if (query instanceof ConstantScoreQuery constant) {
      Query bound = bindChildren(path, constant.getQuery());
      return bound == constant.getQuery() ? constant : new ConstantScoreQuery(bound);
    }
    if (query instanceof DisjunctionMaxQuery disMax) {
      List<Query> disjuncts = new ArrayList<>();
      boolean changed = false;
      for (Query disjunct : disMax.getDisjuncts()) {
        Query bound = bindChildren(path, disjunct);
        changed |= bound != disjunct;
        disjuncts.add(bound);
      }
      return changed
          ? new DisjunctionMaxQuery(disjuncts, disMax.getTieBreakerMultiplier())
          : disMax;
    }
    if (hidesNested(query)) {
      throw new IllegalArgumentException(
          "a nested query inside the nested query on \""
              + path
              + "\" is wrapped in "
              + query.getClass().getName()
              + ", which binding cannot see through; wrap it in a boolean, boost, constant-score"
              + " or dis-max query instead: "
              + query);
    }
    return query;
  }

  /** This query joined to the documents on {@code newParentPath}. */
  private NestedQuery boundTo(String newParentPath) {
    if (!isUnder(path, newParentPath)) {
      throw new IllegalArgumentException(
          "the nested query on \""
              + path
              + "\" is inside the nested query on \""
              + newParentPath
              + "\", so its path must start with \""
              + newParentPath
              + ".\"");
    }
    if (newParentPath.equals(parentPath)) {
      return this;
    }
    return new NestedQuery(path, childQuery, scoreMode, newParentPath, innerHits, null);
  }

  /** Whether a nested query is reachable from {@code query} by way of {@link Query#visit}. */
  private static boolean hidesNested(Query query) {
    NestedFinder finder = new NestedFinder();
    query.visit(finder);
    return finder.found;
  }

  /**
   * Finds a NestedQuery anywhere under the visited query.
   *
   * <p>Overrides two defaults that would hide one: {@code getSubVisitor} normally skips MUST_NOT
   * clauses, and a nested query is bound the same under one; and Lucene's block-join queries only
   * report themselves as leaves, so their inner query is visited here explicitly.
   */
  private static final class NestedFinder extends QueryVisitor {
    boolean found;

    @Override
    public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
      if (parent instanceof NestedQuery) {
        found = true;
      }
      return this;
    }

    @Override
    public void visitLeaf(Query query) {
      if (query instanceof NestedQuery) {
        found = true;
      } else if (query instanceof ToParentBlockJoinQuery join) {
        join.getChildQuery().visit(this);
      } else if (query instanceof ToChildBlockJoinQuery join) {
        join.getParentQuery().visit(this);
      }
    }
  }

  /** The children's path. */
  public String getPath() {
    return path;
  }

  /** What a child must match, with its own nested queries bound to {@link #getPath()}. */
  public Query getChildQuery() {
    return childQuery;
  }

  /** How matching children's scores become the parent's. */
  public ScoreMode getScoreMode() {
    return scoreMode;
  }

  /** The parents' path, or null when the parents are the roots. */
  public String getParentPath() {
    return parentPath;
  }

  /**
   * The inner-hits request, or null for none. Opaque here; carried through binding and part of
   * equality.
   */
  public Object getInnerHits() {
    return innerHits;
  }

  /**
   * The child side of the join: {@link #getChildQuery()} restricted to documents on {@link
   * #getPath()}.
   */
  public Query childLevelQuery() {
    return childLevelQuery;
  }

  /**
   * The nested queries in {@code query} that request inner hits, as a tree in which each node's
   * children are the requests whose hits nest under ITS hits.
   *
   * <p>A request's hits nest under its nearest enclosing nested query that also requests inner
   * hits, as in ES, and under the top-level hit when none does. A nested query in between that
   * requests none is not a level of the tree; it still constrains the hits below it, see {@link
   * InnerHitsNode#getHitsQuery()}.
   *
   * <p>Found through {@link Query#visit}, so through any query that visits its subqueries, but not
   * under a MUST_NOT clause, as in ES: a hit matches there for want of such children, so it has
   * none to show. Not inside Lucene's own block-join queries either, which visit as leaves.
   */
  public static List<InnerHitsNode> innerHitsOf(Query query) {
    List<InnerHitsNode> found = new ArrayList<>();
    query.visit(new InnerHitsFinder(found, List.of()));
    return Collections.unmodifiableList(found);
  }

  /** One inner-hits request in a query, as {@link #innerHitsOf(Query)} finds it. */
  public static final class InnerHitsNode {
    private final NestedQuery query;
    private final Query hitsQuery;
    private final List<InnerHitsNode> children = new ArrayList<>();

    private InnerHitsNode(NestedQuery query, Query hitsQuery) {
      this.query = query;
      this.hitsQuery = hitsQuery;
    }

    /** The nested query carrying the request, bound as it is in the enclosing query. */
    public NestedQuery getQuery() {
      return query;
    }

    /**
     * What a document under the enclosing hit must match to be one of this request's hits: its
     * nested query's {@link NestedQuery#childLevelQuery()}, scored as that alone.
     *
     * <p>A nested query between this one and the enclosing hit's level that requests no inner hits
     * adds a filter: the hit's ancestor on that query's path must match that query's child level,
     * since only through such an ancestor did the enclosing hit match. A block join over the
     * documents on that path finds the ancestor, by the same post-order argument as the join
     * itself.
     */
    public Query getHitsQuery() {
      return hitsQuery;
    }

    /** The requests whose hits nest under this one's, in the order the query visits them. */
    public List<InnerHitsNode> getChildren() {
      return Collections.unmodifiableList(children);
    }
  }

  /**
   * Collects inner-hits requests into {@code found}, the enclosing request's children or the top
   * level, remembering in {@code between} the nested queries passed through since that level.
   */
  private static final class InnerHitsFinder extends QueryVisitor {
    private final List<InnerHitsNode> found;
    private final List<NestedQuery> between;

    InnerHitsFinder(List<InnerHitsNode> found, List<NestedQuery> between) {
      this.found = found;
      this.between = between;
    }

    @Override
    public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
      if (occur == BooleanClause.Occur.MUST_NOT) {
        return QueryVisitor.EMPTY_VISITOR;
      }
      if (!(parent instanceof NestedQuery nested)) {
        return this;
      }
      if (nested.innerHits == null) {
        List<NestedQuery> deeper = new ArrayList<>(between);
        deeper.add(nested);
        return new InnerHitsFinder(found, deeper);
      }
      Query hitsQuery = nested.childLevelQuery;
      if (!between.isEmpty()) {
        BooleanQuery.Builder builder =
            new BooleanQuery.Builder().add(nested.childLevelQuery, BooleanClause.Occur.MUST);
        for (NestedQuery level : between) {
          builder.add(
              new ToChildBlockJoinQuery(level.childLevelQuery, parentFilter(level.path)),
              BooleanClause.Occur.FILTER);
        }
        hitsQuery = builder.build();
      }
      InnerHitsNode node = new InnerHitsNode(nested, hitsQuery);
      found.add(node);
      return new InnerHitsFinder(node.children, List.of());
    }
  }

  /** The block join this stands for, now that its parents are fixed. */
  @Override
  public Query rewrite(IndexSearcher searcher) {
    return new ToParentBlockJoinQuery(childLevelQuery, parentFilter(parentPath), scoreMode);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    childQuery.visit(visitor.getSubVisitor(BooleanClause.Occur.MUST, this));
  }

  @Override
  public String toString(String field) {
    StringBuilder sb =
        new StringBuilder("nested(")
            .append(path)
            .append(" -> ")
            .append(parentPath == null ? "<roots>" : parentPath)
            .append(", ")
            .append(childQuery.toString(field))
            .append(", score_mode=")
            .append(scoreMode);
    if (innerHits != null) {
      sb.append(", inner_hits=").append(innerHits);
    }
    return sb.append(')').toString();
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) && equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(NestedQuery other) {
    return path.equals(other.path)
        && childQuery.equals(other.childQuery)
        && scoreMode == other.scoreMode
        && Objects.equals(parentPath, other.parentPath)
        && Objects.equals(innerHits, other.innerHits);
  }

  @Override
  public int hashCode() {
    return 31 * classHash() + Objects.hash(path, childQuery, scoreMode, parentPath, innerHits);
  }
}
