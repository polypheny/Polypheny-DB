/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.sql;


import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;


/**
 * Named, built-in lexical policy. A lexical policy describes how identifiers are quoted, whether they are converted to upper- or lower-case when they are read,
 * and whether they are matched case-sensitively.
 */
public enum Lex {
    /**
     * Lexical policy similar to Oracle. The case of identifiers enclosed in double-quotes is preserved; unquoted identifiers are converted to upper-case;
     * after which, identifiers are matched case-sensitively.
     */
    ORACLE( Quoting.DOUBLE_QUOTE, Casing.TO_UPPER, Casing.UNCHANGED, true ),

    /**
     * Lexical policy similar to MySQL. (To be precise: MySQL on Windows; MySQL on Linux uses case-sensitive matching, like the Linux file system.) The case of identifiers is preserved whether or not
     * they quoted; after which, identifiers are matched case-insensitively. Back-ticks allow identifiers to contain non-alphanumeric characters.
     */
    MYSQL( Quoting.BACK_TICK, Casing.UNCHANGED, Casing.UNCHANGED, false ),

    /**
     * Lexical policy similar to MySQL with ANSI_QUOTES option enabled. (To be precise: MySQL on Windows; MySQL on Linux uses case-sensitive matching, like the Linux file system.)
     * The case of identifiers is preserved whether or not they quoted; after which, identifiers are matched case-insensitively. Double quotes allow identifiers to contain non-alphanumeric characters.
     */
    MYSQL_ANSI( Quoting.DOUBLE_QUOTE, Casing.UNCHANGED, Casing.UNCHANGED, false ),

    /**
     * Lexical policy similar to Microsoft SQL Server. The case of identifiers is preserved whether or not they are quoted; after which, identifiers are matched case-insensitively.
     * Brackets allow identifiers to contain non-alphanumeric characters.
     */
    SQL_SERVER( Quoting.BRACKET, Casing.UNCHANGED, Casing.UNCHANGED, false ),

    /**
     * Lexical policy similar to Java. The case of identifiers is preserved whether or not they are quoted; after which, identifiers are matched case-sensitively.
     * Unlike Java, back-ticks allow identifiers to contain non-alphanumeric characters.
     */
    JAVA( Quoting.BACK_TICK, Casing.UNCHANGED, Casing.UNCHANGED, true );

    public final Quoting quoting;
    public final Casing unquotedCasing;
    public final Casing quotedCasing;
    public final boolean caseSensitive;


    Lex( Quoting quoting, Casing unquotedCasing, Casing quotedCasing, boolean caseSensitive ) {
        this.quoting = quoting;
        this.unquotedCasing = unquotedCasing;
        this.quotedCasing = quotedCasing;
        this.caseSensitive = caseSensitive;
    }
}
