/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
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
 */

package ch.unibas.dmi.dbis.polyphenydb.util;


import java.io.FilterWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayDeque;
import java.util.Deque;


/**
 * A helper class for generating formatted text. StackWriter keeps track of
 * nested formatting state like indentation level and quote escaping. Typically,
 * it is inserted between a PrintWriter and the real Writer; directives are
 * passed straight through the PrintWriter via the write method, as in the
 * following example:
 *
 * <blockquote><pre><code>
 *    StringWriter sw = new StringWriter();
 *    StackWriter stackw = new StackWriter(sw, StackWriter.INDENT_SPACE4);
 *    PrintWriter pw = new PrintWriter(stackw);
 *    pw.write(StackWriter.INDENT);
 *    pw.print("execute remote(link_name,");
 *    pw.write(StackWriter.OPEN_SQL_STRING_LITERAL);
 *    pw.println();
 *    pw.write(StackWriter.INDENT);
 *    pw.println("select * from t where c &gt; 'alabama'");
 *    pw.write(StackWriter.OUTDENT);
 *    pw.write(StackWriter.CLOSE_SQL_STRING_LITERAL);
 *    pw.println(");");
 *    pw.write(StackWriter.OUTDENT);
 *    pw.close();
 *    System.out.println(sw.toString());
 * </code></pre></blockquote>
 *
 * <p>which produces the following output:
 *
 * <blockquote><pre><code>
 *      execute remote(link_name,'
 *          select * from t where c &gt; ''alabama''
 *      ');
 * </code></pre></blockquote>
 */
public class StackWriter extends FilterWriter {
    //~ Static fields/initializers ---------------------------------------------

    /**
     * directive for increasing the indentation level
     */
    public static final int INDENT = 0xF0000001;

    /**
     * directive for decreasing the indentation level
     */
    public static final int OUTDENT = 0xF0000002;

    /**
     * directive for beginning an SQL string literal
     */
    public static final int OPEN_SQL_STRING_LITERAL = 0xF0000003;

    /**
     * directive for ending an SQL string literal
     */
    public static final int CLOSE_SQL_STRING_LITERAL = 0xF0000004;

    /**
     * directive for beginning an SQL identifier
     */
    public static final int OPEN_SQL_IDENTIFIER = 0xF0000005;

    /**
     * directive for ending an SQL identifier
     */
    public static final int CLOSE_SQL_IDENTIFIER = 0xF0000006;

    /**
     * tab indentation
     */
    public static final String INDENT_TAB = "\t";

    /**
     * four-space indentation
     */
    public static final String INDENT_SPACE4 = "    ";
    private static final Character SINGLE_QUOTE = '\'';
    private static final Character DOUBLE_QUOTE = '"';

    //~ Instance fields --------------------------------------------------------

    private int indentationDepth;
    private String indentation;
    private boolean needIndent;
    private final Deque<Character> quoteStack = new ArrayDeque<>();

    //~ Constructors -----------------------------------------------------------


    /**
     * Creates a new StackWriter on top of an existing Writer, with the
     * specified string to be used for each level of indentation.
     *
     * @param writer underlying writer
     * @param indentation indentation unit such as {@link #INDENT_TAB} or
     * {@link #INDENT_SPACE4}
     */
    public StackWriter( Writer writer, String indentation ) {
        super( writer );
        this.indentation = indentation;
    }

    //~ Methods ----------------------------------------------------------------


    private void indentIfNeeded() throws IOException {
        if ( needIndent ) {
            for ( int i = 0; i < indentationDepth; i++ ) {
                out.write( indentation );
            }
            needIndent = false;
        }
    }


    private void writeQuote( Character quoteChar ) throws IOException {
        indentIfNeeded();
        int n = 1;
        for ( Character quote : quoteStack ) {
            if ( quote.equals( quoteChar ) ) {
                n *= 2;
            }
        }
        for ( int i = 0; i < n; i++ ) {
            out.write( quoteChar );
        }
    }


    private void pushQuote( Character quoteChar ) throws IOException {
        writeQuote( quoteChar );
        quoteStack.push( quoteChar );
    }


    private void popQuote( Character quoteChar ) throws IOException {
        final Character pop = quoteStack.pop();
        assert pop.equals( quoteChar );
        writeQuote( quoteChar );
    }


    // implement Writer
    public void write( int c ) throws IOException {
        switch ( c ) {
            case INDENT:
                indentationDepth++;
                break;
            case OUTDENT:
                indentationDepth--;
                break;
            case OPEN_SQL_STRING_LITERAL:
                pushQuote( SINGLE_QUOTE );
                break;
            case CLOSE_SQL_STRING_LITERAL:
                popQuote( SINGLE_QUOTE );
                break;
            case OPEN_SQL_IDENTIFIER:
                pushQuote( DOUBLE_QUOTE );
                break;
            case CLOSE_SQL_IDENTIFIER:
                popQuote( DOUBLE_QUOTE );
                break;
            case '\n':
                out.write( c );
                needIndent = true;
                break;
            case '\r':

                // NOTE jvs 3-Jan-2006:  suppress indentIfNeeded() in this case
                // so that we don't get spurious diffs on Windows vs. Linux
                out.write( c );
                break;
            case '\'':
                writeQuote( SINGLE_QUOTE );
                break;
            case '"':
                writeQuote( DOUBLE_QUOTE );
                break;
            default:
                indentIfNeeded();
                out.write( c );
                break;
        }
    }


    // implement Writer
    public void write( char[] cbuf, int off, int len ) throws IOException {
        // TODO: something more efficient using searches for
        // special characters
        for ( int i = off; i < (off + len); i++ ) {
            write( cbuf[i] );
        }
    }


    // implement Writer
    public void write( String str, int off, int len ) throws IOException {
        // TODO: something more efficient using searches for
        // special characters
        for ( int i = off; i < (off + len); i++ ) {
            write( str.charAt( i ) );
        }
    }


    /**
     * Writes an SQL string literal.
     *
     * @param pw PrintWriter on which to write
     * @param s text of literal
     */
    public static void printSqlStringLiteral( PrintWriter pw, String s ) {
        pw.write( OPEN_SQL_STRING_LITERAL );
        pw.print( s );
        pw.write( CLOSE_SQL_STRING_LITERAL );
    }


    /**
     * Writes an SQL identifier.
     *
     * @param pw PrintWriter on which to write
     * @param s identifier
     */
    public static void printSqlIdentifier( PrintWriter pw, String s ) {
        pw.write( OPEN_SQL_IDENTIFIER );
        pw.print( s );
        pw.write( CLOSE_SQL_IDENTIFIER );
    }
}

// End StackWriter.java
