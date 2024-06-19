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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

/**
 * Utilities for creating strings of spaces.
 */
public class Spaces {

    /**
     * The longest possible string of spaces. Fine as long as you don't try
     * to print it.
     *
     * <p>Use with {@link StringBuilder#append(CharSequence, int, int)} to
     * append spaces without doing memory allocation.</p>
     */
    public static final CharSequence MAX = sequence( Integer.MAX_VALUE );


    // Utility class. Do not instantiate.
    private Spaces() {
    }


    /**
     * Creates a sequence of {@code n} spaces.
     */
    public static CharSequence sequence( int n ) {
        return new Spaces.SpaceString( n );
    }


    /**
     * Returns a string of {@code n} spaces.
     */
    public static String of( int n ) {
        return StringUtils.rightPad( "", n );
    }


    /**
     * Appends {@code n} spaces to an {@link Appendable}.
     */
    public static Appendable append( Appendable buf, int n ) throws IOException {
        buf.append( MAX, 0, n );
        return buf;
    }


    /**
     * Appends {@code n} spaces to a {@link PrintWriter}.
     */
    public static PrintWriter append( PrintWriter pw, int n ) {
        pw.append( MAX, 0, n );
        return pw;
    }


    /**
     * Appends {@code n} spaces to a {@link StringWriter}.
     */
    public static StringWriter append( StringWriter pw, int n ) {
        pw.append( MAX, 0, n );
        return pw;
    }


    /**
     * Appends {@code n} spaces to a {@link StringBuilder}.
     */
    public static StringBuilder append( StringBuilder buf, int n ) {
        buf.append( MAX, 0, n );
        return buf;
    }


    /**
     * Appends {@code n} spaces to a {@link StringBuffer}.
     */
    public static StringBuffer append( StringBuffer buf, int n ) {
        buf.append( MAX, 0, n );
        return buf;
    }


    /**
     * Returns a string that is padded on the right with spaces to the given
     * length.
     */
    public static String padRight( String string, int n ) {
        final int x = n - string.length();
        if ( x <= 0 ) {
            return string;
        }
        // Replacing StringBuffer with String would hurt performance.
        return append( new StringBuilder( string ), x ).toString();
    }


    /**
     * A string of spaces.
     */
    private record SpaceString( int length ) implements CharSequence {

        // Do not override equals and hashCode to be like String. CharSequence does
        // not require it.


        @SuppressWarnings("NullableProblems")
        @Override
        public String toString() {
            return of( length );
        }


        public char charAt( int index ) {
            return ' ';
        }


        public @NotNull CharSequence subSequence( int start, int end ) {
            return StringUtils.rightPad( "", end - start );
        }

    }

}
