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

package org.polypheny.db.util;

import static org.polypheny.db.util.Static.RESOURCE;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Locale;
import lombok.Getter;

public class Collation implements Serializable {

    public static final Collation IMPLICIT = new Collation( Coercibility.IMPLICIT );
    public static final Collation COERCIBLE = new Collation( Coercibility.COERCIBLE );
    @Getter
    protected final String collationName;
    protected final SerializableCharset wrappedCharset;
    protected final Locale locale;
    protected final String strength;
    @Getter
    protected final Coercibility coercibility;


    /**
     * Creates a SqlCollation with the default collation name and the given credibility.
     *
     * @param coercibility Coercibility
     */
    public Collation( Coercibility coercibility ) {
        this( SaffronProperties.INSTANCE.defaultCollation().get(), coercibility );
    }


    /**
     * Creates a Collation by its name and its coercibility
     *
     * @param collation Collation specification
     * @param coercibility Coercibility
     */
    public Collation( String collation, Coercibility coercibility ) {
        this.coercibility = coercibility;
        CoreUtil.ParsedCollation parseValues = CoreUtil.parseCollation( collation );
        Charset charset = parseValues.getCharset();
        this.wrappedCharset = SerializableCharset.forCharset( charset );
        locale = parseValues.getLocale();
        strength = parseValues.getStrength();
        String c = charset.name().toUpperCase( Locale.ROOT ) + "$" + locale.toString();
        if ( (strength != null) && (!strength.isEmpty()) ) {
            c += "$" + strength;
        }
        collationName = c;
    }


    public boolean equals( Object o ) {
        return this == o
                || o instanceof Collation
                && collationName.equals( ((Collation) o).collationName );
    }


    @Override
    public int hashCode() {
        return collationName.hashCode();
    }


    public String toString() {
        return "COLLATE " + collationName;
    }


    public Charset getCharset() {
        return wrappedCharset.getCharset();
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
     * Returns the result for {#@link #getCoercibilityDyadicComparison} and {@link #getCoercibilityDyadicOperator}.
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
                        if ( col1.collationName.equals( col2.collationName ) ) {
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
                        if ( col1.collationName.equals( col2.collationName ) ) {
                            yield col2;
                        }
                        throw RESOURCE.differentCollations( col1.collationName, col2.collationName ).ex();
                    }
                };
            default:
                throw Util.unexpected( coercibility1 );
        }
    }


    /**
     * <blockquote>A &lt;character value expression&gt; consisting of a column reference has the coercibility characteristic Implicit, with collating
     * sequence as defined when the column was created. A &lt;character value expression&gt; consisting of a value other than a column (e.g., a host
     * variable or a literal) has the coercibility characteristic Coercible, with the default collation for its character repertoire. A &lt;character
     * value expression&gt; simply containing a &lt;collate clause&gt; has the coercibility characteristic Explicit, with the collating sequence
     * specified in the &lt;collate clause&gt;.</blockquote>
     *
     * @see Glossary#SQL99 SQL:1999 Part 2 Section 4.2.3
     */
    public enum Coercibility {
        /**
         * Strongest coercibility.
         */
        EXPLICIT,
        IMPLICIT,
        COERCIBLE,
        /**
         * Weakest coercibility.
         */
        NONE
    }

}
