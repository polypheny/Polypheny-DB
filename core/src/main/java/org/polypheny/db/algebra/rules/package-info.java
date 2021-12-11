
/**
 * Provides a core set of planner rules.
 *
 * Consider this package to be the "standard library" of planner rules.
 * Most of the common rewrites that you would want to perform on logical relational expressions, or generically on any data source, are present, and have been well tested.
 *
 * Of course, the library is never complete, and contributions are welcome.
 *
 * Not present are rules specific to a particular data source: look in that data source's adapter.
 *
 * Also out of the scope of this package are rules that support a particular operation, such as decorrelation. Those are defined along with the algorithm.
 *
 * For
 *
 * <h2>Related packages and classes</h2>
 * <ul>
 * <li>Package<code> <a href="../../sql/package-summary.html">org.polypheny.db.sql</a></code> is an object model for SQL expressions</li>
 * <li>Package<code> <a href="../../rex/package-summary.html">org.polypheny.db.rex</a></code> is an object model for relational row expressions</li>
 * <li>Package<code> <a href="../../plan/package-summary.html">org.polypheny.db.plan</a></code> provides an optimizer interface.</li>
 * </ul>
 */

package org.polypheny.db.algebra.rules;

