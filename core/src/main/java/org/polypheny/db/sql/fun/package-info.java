
/**
 * Defines the set of standard SQL row-level functions and operators.
 *
 * The standard set of row-level functions and operators are declared in class {@link org.polypheny.db.sql.fun.SqlStdOperatorTable}. Anonymous inner classes within that
 * table are allowed only for specifying an operator's test function; if other custom code is needed for an operator, it should be implemented in a top-level class within this
 * package instead.  Operators which are not row-level (e.g. select and join) should be defined in package {@code org.polypheny.db.sql} instead.
 */

package org.polypheny.db.sql.fun;

