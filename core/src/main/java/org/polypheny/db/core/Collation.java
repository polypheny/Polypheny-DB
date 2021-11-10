/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.core;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Locale;
import lombok.Getter;
import org.polypheny.db.util.Glossary;
import org.polypheny.db.util.SaffronProperties;
import org.polypheny.db.util.SerializableCharset;

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
        if ( (strength != null) && (strength.length() > 0) ) {
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
