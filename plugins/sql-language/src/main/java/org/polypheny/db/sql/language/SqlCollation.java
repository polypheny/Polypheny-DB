/*
 * Copyright 2019-2024 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.sql.language;


import static org.polypheny.db.util.Static.RESOURCE;

import org.polypheny.db.runtime.PolyphenyDbException;
import org.polypheny.db.runtime.PolyphenyDbResource;
import org.polypheny.db.util.Collation;
import org.polypheny.db.util.Glossary;
import org.polypheny.db.util.Util;


/**
 * A <code>SqlCollation</code> is an object representing a <code>Collate</code> statement. It is immutable.
 */
public class SqlCollation extends Collation {


    public SqlCollation( String collation, Coercibility coercibility ) {
        super( collation, coercibility );
    }


    /**
     * Returns the collating sequence (the collation name) and the coercibility for the resulting value of a dyadic operator.
     *
     * @param col1 first operand for the dyadic operation
     * @param col2 second operand for the dyadic operation
     * @return the resulting collation sequence. The "no collating sequence" result is returned as null.
     * @see Glossary#SQL99 SQL:1999 Part 2 Section 4.2.3 Table 2
     */
    public static Collation getCoercibilityDyadicOperator( Collation col1, Collation col2 ) {
        return getCoercibilityDyadic( col1, col2 );
    }


    /**
     * Returns the collating sequence (the collation name) and the coercibility for the resulting value of a dyadic operator.
     *
     * @param col1 first operand for the dyadic operation
     * @param col2 second operand for the dyadic operation
     * @return the resulting collation sequence
     * @throws PolyphenyDbException from {@link PolyphenyDbResource#invalidCompare} or {@link PolyphenyDbResource#differentCollations} if no collating sequence can be deduced
     * @see Glossary#SQL99 SQL:1999 Part 2 Section 4.2.3 Table 2
     */
    public static Collation getCoercibilityDyadicOperatorThrows( Collation col1, Collation col2 ) {
        Collation ret = getCoercibilityDyadic( col1, col2 );
        if ( null == ret ) {
            throw RESOURCE.invalidCompare(
                    col1.getCollationName(),
                    "" + col1.getCoercibility(),
                    col2.getCollationName(),
                    "" + col2.getCoercibility() ).ex();
        }
        return ret;
    }


    /**
     * Returns the collating sequence (the collation name) to use for the resulting value of a comparison.
     *
     * @param col1 first operand for the dyadic operation
     * @param col2 second operand for the dyadic operation
     * @return the resulting collation sequence. If no collating sequence could be deduced throws a {@link PolyphenyDbResource#invalidCompare}
     * @see Glossary#SQL99 SQL:1999 Part 2 Section 4.2.3 Table 3
     */
    public static String getCoercibilityDyadicComparison( Collation col1, Collation col2 ) {
        return getCoercibilityDyadicOperatorThrows( col1, col2 ).getCollationName();
    }


    /**
     * Returns the result for {@link #getCoercibilityDyadicComparison} and {@link #getCoercibilityDyadicOperator}.
     */
    protected static Collation getCoercibilityDyadic( Collation col1, Collation col2 ) {
        assert null != col1;
        assert null != col2;
        final Coercibility coercibility1 = col1.getCoercibility();
        final Coercibility coercibility2 = col2.getCoercibility();
        switch ( coercibility1 ) {
            case COERCIBLE:
                return switch ( coercibility2 ) {
                    case COERCIBLE -> col2;
                    case IMPLICIT -> col2;
                    case NONE -> null;
                    case EXPLICIT -> col2;
                };
            case IMPLICIT:
                return switch ( coercibility2 ) {
                    case COERCIBLE -> col1;
                    case IMPLICIT -> {
                        if ( col1.getCollationName().equals( col2.getCollationName() ) ) {
                            yield col2;
                        }
                        yield null;
                    }
                    case NONE -> null;
                    case EXPLICIT -> col2;
                };
            case NONE:
                return switch ( coercibility2 ) {
                    case COERCIBLE, IMPLICIT, NONE -> null;
                    case EXPLICIT -> col2;
                };
            case EXPLICIT:
                return switch ( coercibility2 ) {
                    case COERCIBLE, IMPLICIT, NONE -> col1;
                    case EXPLICIT -> {
                        if ( col1.getCollationName().equals( col2.getCollationName() ) ) {
                            yield col2;
                        }
                        throw RESOURCE.differentCollations( col1.getCollationName(), col2.getCollationName() ).ex();
                    }
                };
            default:
                throw Util.unexpected( coercibility1 );
        }
    }


    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "COLLATE" );
        writer.identifier( collationName );
    }


}
