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
 *
 * This file incorporates code covered by the following terms:
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
 */

package org.polypheny.db.util;


import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.AbstractCollection;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.RandomAccess;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collector;
import javax.annotation.Nonnull;
import org.apache.calcite.linq4j.Ord;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.Monotonicity;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.nodes.BasicNodeVisitor;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.Literal;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.RowOperator;
import org.polypheny.db.nodes.ValuesOperator;
import org.polypheny.db.util.temporal.DateTimeUtils;
import org.slf4j.Logger;


/**
 * Miscellaneous utility functions.
 */
public class Util {

    private Util() {
    }


    /**
     * System-dependent newline character.
     * <p>
     * In general, you should not use this in expected results of tests.
     * Expected results should be the expected result on Linux (or Mac OS) using '\n'. Apply {@link Util#toLinux(String)} to Windows actual results, if
     * necessary, to make them look like Linux actual.
     */
    public static final String LINE_SEPARATOR = System.getProperty( "line.separator" );

    /**
     * System-dependent file separator, for example, "/" or "\."
     */
    public static final String FILE_SEPARATOR = System.getProperty( "file.separator" );

    /**
     * Datetime format string for generating a timestamp string to be used as part of a filename. Conforms to SimpleDateFormat conventions.
     */
    public static final String FILE_TIMESTAMP_FORMAT = "yyyy-MM-dd_HH_mm_ss";

    /**
     * Regular expression for a valid java identifier which contains no underscores and can therefore be returned intact by {@link #toJavaId}.
     */
    private static final Pattern JAVA_ID_PATTERN = Pattern.compile( "[a-zA-Z_$][a-zA-Z0-9$]*" );

    private static final Charset DEFAULT_CHARSET = Charsets.UTF_8;//Charset.forName( SaffronProperties.INSTANCE.defaultCharset().get() );

    /**
     * Maps classes to the map of their enum values. Uses a weak map so that classes are not prevented from being unloaded.
     */
    @SuppressWarnings("unchecked")
    private static final LoadingCache<Class, Map<String, Enum>> ENUM_CONSTANTS =
            CacheBuilder.newBuilder()
                    .weakKeys()
                    .build( CacheLoader.from( Util::enumConstants ) );


    /**
     * Does nothing with its argument. Returns whether it is ensured that the call produces a single value
     *
     * @param call the expression to evaluate
     * @return Whether it is ensured that the call produces a single value
     */
    public static boolean isSingleValue( Call call ) {
        if ( call.getOperator() instanceof AggFunction ) {
            return true;
        } else if ( call.getOperator() instanceof ValuesOperator || call.getOperator() instanceof RowOperator ) {
            List<Node> operands = call.getOperandList();
            if ( operands.size() == 1 ) {
                Node operand = operands.get( 0 );
                if ( operand instanceof Literal ) {
                    return true;
                } else if ( operand instanceof Call ) {
                    return isSingleValue( (Call) operand );
                }
            }

            return false;
        } else {
            boolean isScalar = true;
            for ( Node operand : call.getOperandList() ) {
                if ( operand instanceof Literal ) {
                    continue;
                }

                if ( !(operand instanceof Call) || !Util.isSingleValue( (Call) operand ) ) {
                    isScalar = false;
                    break;
                }
            }

            return isScalar;
        }
    }


    /**
     * Does nothing with its argument. Call this method when you have a value you are not interested in, but you don't want the
     * compiler to warn that you are not using it.
     */
    public static void discard( Object o ) {
        if ( false ) {
            discard( o );
        }
    }


    /**
     * Does nothing with its argument. Call this method when you have a value you are not interested in, but you don't want the compiler to warn that
     * you are not using it.
     */
    public static void discard( int i ) {
        if ( false ) {
            discard( i );
        }
    }


    /**
     * Does nothing with its argument. Call this method when you have a value you are not interested in, but you don't want the compiler to warn that
     * you are not using it.
     */
    public static void discard( boolean b ) {
        if ( false ) {
            discard( b );
        }
    }


    /**
     * Does nothing with its argument. Call this method when you have a value you are not interested in, but you don't want the compiler to warn that
     * you are not using it.
     */
    public static void discard( double d ) {
        if ( false ) {
            discard( d );
        }
    }


    /**
     * Records that an exception has been caught but will not be re-thrown. If the tracer is not null, logs the exception to the tracer.
     *
     * @param e Exception
     * @param logger If not null, logs exception to this logger
     */
    public static void swallow( Throwable e, Logger logger ) {
        if ( logger != null ) {
            logger.debug( "Discarding exception", e );
        }
    }


    /**
     * Returns whether two lists are equal to each other using shallow comparisons.
     *
     * @param list0 First list
     * @param list1 Second list
     * @return Whether lists are same length and all of their elements are equal using {@code ==} (may be null).
     */
    public static <T> boolean equalShallow( List<? extends T> list0, List<? extends T> list1 ) {
        if ( list0.size() != list1.size() ) {
            return false;
        }
        for ( int i = 0; i < list0.size(); i++ ) {
            if ( list0.get( i ) != list1.get( i ) ) {
                return false;
            }
        }
        return true;
    }


    /**
     * Returns a set of the elements which are in <code>set1</code> but not in <code>set2</code>, without modifying either.
     */
    public static <T> Set<T> minus( Set<T> set1, Set<T> set2 ) {
        if ( set1.isEmpty() ) {
            return set1;
        } else if ( set2.isEmpty() ) {
            return set1;
        } else {
            Set<T> set = new HashSet<>( set1 );
            set.removeAll( set2 );
            return set;
        }
    }


    /**
     * Computes <code>nlogn(n)</code> using the natural logarithm (or <code>n</code> if <code>n &lt; {@link Math#E}</code>,
     * so the result is never negative.
     */
    public static double nLogN( double d ) {
        return (d < Math.E) ? d : (d * Math.log( d ));
    }


    /**
     * Prints an object using reflection. We can handle <code>null</code>;
     * arrays of objects and primitive values; for regular objects, we print all public fields.
     */
    public static void print( PrintWriter pw, Object o ) {
        print( pw, o, 0 );
    }


    public static void print( PrintWriter pw, Object o, int indent ) {
        if ( o == null ) {
            pw.print( "null" );
            return;
        }
        Class<?> clazz = o.getClass();
        if ( o instanceof String ) {
            printJavaString( pw, (String) o, true );
        } else if (
                (clazz == Integer.class)
                        || (clazz == Boolean.class)
                        || (clazz == Character.class)
                        || (clazz == Byte.class)
                        || (clazz == Short.class)
                        || (clazz == Long.class)
                        || (clazz == Float.class)
                        || (clazz == Double.class)
                        || (clazz == Void.class) ) {
            pw.print( o );
        } else if ( clazz.isArray() ) {
            // o is an array, but we can't cast to Object[] because it may be an array of primitives.
            Object[] a; // for debug
            if ( o instanceof Object[] ) {
                a = (Object[]) o;
                discard( a );
            }
            int n = Array.getLength( o );
            pw.print( "{" );
            for ( int i = 0; i < n; i++ ) {
                if ( i > 0 ) {
                    pw.println( "," );
                } else {
                    pw.println();
                }
                for ( int j = 0; j < indent; j++ ) {
                    pw.print( "\t" );
                }
                print( pw, Array.get( o, i ), indent + 1 );
            }
            pw.print( "}" );
        } else if ( o instanceof Iterator iter ) {
            pw.print( clazz.getName() );
            pw.print( " {" );
            int i = 0;
            while ( iter.hasNext() ) {
                if ( i++ > 0 ) {
                    pw.println( "," );
                }
                print( pw, iter.next(), indent + 1 );
            }
            pw.print( "}" );
        } else if ( o instanceof Enumeration e ) {
            pw.print( clazz.getName() );
            pw.print( " {" );
            int i = 0;
            while ( e.hasMoreElements() ) {
                if ( i++ > 0 ) {
                    pw.println( "," );
                }
                print( pw, e.nextElement(), indent + 1 );
            }
            pw.print( "}" );
        } else {
            pw.print( clazz.getName() );
            pw.print( " {" );
            Field[] fields = clazz.getFields();
            int printed = 0;
            for ( Field field : fields ) {
                if ( Modifier.isStatic( field.getModifiers() ) ) {
                    continue;
                }
                if ( printed++ > 0 ) {
                    pw.println( "," );
                } else {
                    pw.println();
                }
                for ( int j = 0; j < indent; j++ ) {
                    pw.print( "\t" );
                }
                pw.print( field.getName() );
                pw.print( "=" );
                Object val;
                try {
                    val = field.get( o );
                } catch ( IllegalAccessException e ) {
                    throw new GenericRuntimeException( e );
                }
                print( pw, val, indent + 1 );
            }
            pw.print( "}" );
        }
    }


    /**
     * Prints a string, enclosing in double quotes (") and escaping if necessary. For examples, <code>printDoubleQuoted(w,"x\"y",false)</code> prints <code>"x\"y"</code>.
     */
    public static void printJavaString( PrintWriter pw, String s, boolean nullMeansNull ) {
        if ( s == null ) {
            if ( nullMeansNull ) {
                pw.print( "null" );
            }
        } else {
            String s1 = replace( s, "\\", "\\\\" );
            String s2 = replace( s1, "\"", "\\\"" );
            String s3 = replace( s2, "\n\r", "\\n" );
            String s4 = replace( s3, "\n", "\\n" );
            String s5 = replace( s4, "\r", "\\r" );
            pw.print( "\"" );
            pw.print( s5 );
            pw.print( "\"" );
        }
    }


    public static void println( PrintWriter pw, Object o ) {
        print( pw, o, 0 );
        pw.println();
    }


    /**
     * Formats a {@link BigDecimal} value to a string in scientific notation For example
     *
     * <ul>
     * <li>A value of 0.00001234 would be formated as <code>1.234E-5</code></li>
     * <li>A value of 100000.00 would be formated as <code>1.00E5</code></li>
     * <li>A value of 100 (scale zero) would be formated as
     * <code>1E2</code></li>
     * </ul>
     *
     * If <code>bd</code> has a precision higher than 20, this method will truncate the output string to have a precision of 20 (no rounding will be
     * done, just a truncate).
     */
    public static String toScientificNotation( BigDecimal bd ) {
        final int truncateAt = 20;
        String unscaled = bd.unscaledValue().toString();
        if ( bd.signum() < 0 ) {
            unscaled = unscaled.substring( 1 );
        }
        int len = unscaled.length();
        int scale = bd.scale();
        int e = len - scale - 1;

        StringBuilder ret = new StringBuilder();
        if ( bd.signum() < 0 ) {
            ret.append( '-' );
        }

        // do truncation
        unscaled = unscaled.substring( 0, Math.min( truncateAt, len ) );
        ret.append( unscaled.charAt( 0 ) );
        if ( scale == 0 ) {
            // trim trailing zeroes since they aren't significant
            int i = unscaled.length();
            while ( i > 1 ) {
                if ( unscaled.charAt( i - 1 ) != '0' ) {
                    break;
                }
                --i;
            }
            unscaled = unscaled.substring( 0, i );
        }
        if ( unscaled.length() > 1 ) {
            ret.append( "." );
            ret.append( unscaled.substring( 1 ) );
        }

        ret.append( "E" );
        ret.append( e );
        return ret.toString();
    }


    /**
     * Replaces every occurrence of <code>find</code> in <code>s</code> with <code>replace</code>.
     */
    public static String replace( String s, String find, String replace ) {
        // let's be optimistic
        int found = s.indexOf( find );
        if ( found == -1 ) {
            return s;
        }
        StringBuilder sb = new StringBuilder( s.length() );
        int start = 0;
        for ( ; ; ) {
            for ( ; start < found; start++ ) {
                sb.append( s.charAt( start ) );
            }
            if ( found == s.length() ) {
                break;
            }
            sb.append( replace );
            start += find.length();
            found = s.indexOf( find, start );
            if ( found == -1 ) {
                found = s.length();
            }
        }
        return sb.toString();
    }


    /**
     * Converts an arbitrary string into a string suitable for use as a Java identifier.
     * <p>
     * The mapping is one-to-one (that is, distinct strings will produce distinct java identifiers). The mapping is also reversible,
     * but the inverse mapping is not implemented.
     * <p>
     * A valid Java identifier must start with a Unicode letter, underscore, or dollar sign ($). The other characters, if any,
     * can be a Unicode letter, underscore, dollar sign, or digit.
     * <p>
     * This method uses an algorithm similar to URL encoding. Valid characters are unchanged; invalid characters are converted to an
     * underscore followed by the hex code of the character; and underscores are doubled.
     * <p>
     * Examples:
     *
     * <ul>
     * <li><code>toJavaId("foo")</code> returns <code>"foo"</code></li>
     * <li><code>toJavaId("foo bar")</code> returns <code>"foo_20_bar"</code></li>
     * <li><code>toJavaId("foo_bar")</code> returns <code>"foo__bar"</code></li>
     * <li><code>toJavaId("0bar")</code> returns <code>"_40_bar"</code> (digits are illegal as a prefix)</li>
     * <li><code>toJavaId("foo0bar")</code> returns <code>"foo0bar"</code></li>
     * </ul>
     */
    public static String toJavaId( String s, int ordinal ) {
        // If it's already a valid Java id (and doesn't contain any underscores), return it unchanged.
        if ( JAVA_ID_PATTERN.matcher( s ).matches() ) {
            // prepend "ID$" to string so it doesn't clash with java keywords
            return "ID$" + ordinal + "$" + s;
        }

        // Escape underscores and other undesirables.
        StringBuilder buf = new StringBuilder( s.length() + 10 );
        buf.append( "ID$" );
        buf.append( ordinal );
        buf.append( "$" );
        for ( int i = 0; i < s.length(); i++ ) {
            char c = s.charAt( i );
            if ( c == '_' ) {
                buf.append( "__" );
            } else if (
                    (c < 0x7F) /* Normal ascii character */
                            && !Character.isISOControl( c )
                            && ((i == 0) ? Character.isJavaIdentifierStart( c )
                            : Character.isJavaIdentifierPart( c )) ) {
                buf.append( c );
            } else {
                buf.append( "_" );
                buf.append( Integer.toString( c, 16 ) );
                buf.append( "_" );
            }
        }
        return buf.toString();
    }


    public static String toLinux( String s ) {
        return s.replaceAll( "\r\n", "\n" );
    }


    /**
     * @return true if s==null or if s.length()==0
     */
    public static boolean isNullOrEmpty( String s ) {
        return (null == s) || (s.isEmpty());
    }


    /**
     * Converts a list of a string, with a given separator between elements.
     */
    public static <T> String sepList( List<T> list, CharSequence sep ) {
        return String.join( sep, list.stream().map( Object::toString ).toList() );
    }


    /**
     * Returns the {@link Charset} object representing the value of {@link SaffronProperties#defaultCharset}
     *
     * @throws java.nio.charset.IllegalCharsetNameException If the given charset name is illegal
     * @throws java.nio.charset.UnsupportedCharsetException If no support for the named charset is available in this instance of the Java virtual machine
     */
    public static Charset getDefaultCharset() {
        return DEFAULT_CHARSET;
    }


    /**
     * As {@link Throwables}{@code .throwIfUnchecked(Throwable)},
     * which was introduced in Guava 20, but we don't require Guava version 20 yet.
     */
    public static void throwIfUnchecked( Throwable throwable ) {
        Objects.requireNonNull( throwable );
        if ( throwable instanceof RuntimeException ) {
            throw (RuntimeException) throwable;
        }
        if ( throwable instanceof Error ) {
            throw (Error) throwable;
        }
    }


    /**
     * Returns a {@link java.lang.RuntimeException} indicating that a particular feature has not been implemented, but should be.
     * <p>
     * If every 'hole' in our functionality uses this method, it will be easier for us to identity the holes. Throwing a
     * {@link java.lang.UnsupportedOperationException} isn't as good, because sometimes we actually want to partially implement an API.
     * <p>
     * Example usage:
     *
     * <blockquote>
     * <pre><code>class MyVisitor extends BaseVisitor {
     *     void accept(Foo foo) {
     *         // Exception will identify which subclass forgot to override
     *         // this method
     *         throw Util.needToImplement(this);
     *     }
     * }</code></pre>
     * </blockquote>
     *
     * @param o The object which was the target of the call, or null. Passing the object gives crucial information if a method needs to be overridden and a subclass forgot to do so.
     * @return an {@link UnsupportedOperationException}.
     */
    public static RuntimeException needToImplement( Object o ) {
        String description = null;
        if ( o != null ) {
            description = o.getClass() + ": " + o;
        }
        throw new UnsupportedOperationException( description );
    }


    /**
     * Returns whether an array of strings contains a given string among the first <code>length</code> entries.
     *
     * @param a Array of strings
     * @param length Number of entries to search
     * @param s String to seek
     * @return Whether array contains the name
     */
    public static boolean contains( String[] a, int length, String s ) {
        for ( int i = 0; i < length; i++ ) {
            if ( a[i].equals( s ) ) {
                return true;
            }
        }
        return false;
    }


    /**
     * Converts an iterable to a string.
     */
    public static <T> String toString( Iterable<T> iterable, String start, String sep, String end ) {
        final StringBuilder buf = new StringBuilder();
        buf.append( start );
        for ( Ord<T> ord : Ord.zip( iterable ) ) {
            if ( ord.i > 0 ) {
                buf.append( sep );
            }
            buf.append( ord.e );
        }
        buf.append( end );
        return buf.toString();
    }


    /**
     * Converts a list of strings to a string separated by newlines.
     */
    public static String lines( Iterable<String> strings ) {
        return toString( strings, "", "\n", "" );
    }


    /**
     * Converts a string into tokens.
     */
    public static Iterable<String> tokenize( final String s, final String delim ) {
        return new Iterable<>() {
            final StringTokenizer t = new StringTokenizer( s, delim );


            @Override
            public Iterator<String> iterator() {
                return new Iterator<String>() {
                    @Override
                    public boolean hasNext() {
                        return t.hasMoreTokens();
                    }


                    @Override
                    public String next() {
                        return t.nextToken();
                    }


                };
            }
        };
    }


    /**
     * Converts a Java timezone to POSIX format, so that the boost C++ library can instantiate timezone objects.
     * <p>
     * <a href="http://www.opengroup.org/onlinepubs/000095399/basedefs/xbd_chap08.html">POSIX IEEE 1003.1</a> defines a format for timezone specifications.
     * <p>
     * The boost C++ library can read these specifications and instantiate <a href="http://www.boost.org/doc/html/date_time/local_time.html#date_time.local_time.posix_time_zone"> posix_time_zone</a>
     * objects from them. The purpose of this method, therefore, is to allow the C++ code such as the fennel calculator to use the same notion of timezone as Java code.
     * <p>
     * The format is as follows:
     *
     * <blockquote>"std offset dst [offset],start[/time],end[/time]"</blockquote>
     *
     * where:
     *
     * <ul>
     * <li>'std' specifies the abbrev of the time zone.
     * <li>'offset' is the offset from UTC, and takes the form <code>[+|-]hh[:mm[:ss]] {h=0-23, m/s=0-59}</code></li>
     * <li>'dst' specifies the abbrev of the time zone during daylight savings time
     * <li>The second offset is how many hours changed during DST. Default=1
     * <li>'start' and 'end' are the dates when DST goes into (and out of) effect.<br>
     * <br>
     * They can each be one of three forms:
     *
     * <ol>
     * <li>Mm.w.d {month=1-12, week=1-5 (5 is always last), day=0-6}
     * <li>Jn {n=1-365 Feb29 is never counted}
     * <li>n {n=0-365 Feb29 is counted in leap years}
     * </ol>
     * </li>
     *
     * <li>'time' has the same format as 'offset', and defaults to 02:00:00.</li>
     * </ul>
     *
     * <p>For example:</p>
     *
     * <ul>
     * <li>"PST-8PDT01:00:00,M4.1.0/02:00:00,M10.1.0/02:00:00"; or more tersely
     * <li>"PST-8PDT,M4.1.0,M10.1.0"
     * </ul>
     *
     * (Real format strings do not contain spaces; they are in the above template only for readability.)
     * <p>
     * Boost apparently diverges from the POSIX standard in how it treats the sign of timezone offsets. The POSIX standard states '<i>If preceded by a
     * '-', the timezone shall be east of the Prime Meridian; otherwise, it shall be west</i>', yet boost requires the opposite. For instance, PST
     * has offset '-8' above. This method generates timezone strings consistent with boost's expectations.
     *
     * @param tz Timezone
     * @param verbose Whether to include fields which can be omitted because they have their default values
     * @return Timezone in POSIX format (offset sign reversed, per boost's idiosyncracies)
     */
    public static String toPosix( TimeZone tz, boolean verbose ) {
        StringBuilder buf = new StringBuilder();
        buf.append( tz.getDisplayName( false, TimeZone.SHORT, Locale.ROOT ) );
        appendPosixTime( buf, tz.getRawOffset() );
        final int dstSavings = tz.getDSTSavings();
        if ( dstSavings == 0 ) {
            return buf.toString();
        }
        buf.append( tz.getDisplayName( true, TimeZone.SHORT, Locale.ROOT ) );
        if ( verbose || (dstSavings != 3600000) ) {
            // POSIX allows us to omit DST offset if it is 1:00:00
            appendPosixTime( buf, dstSavings );
        }
        String patternString = ".*,"
                + "startMode=([0-9]*),"
                + "startMonth=([0-9]*),"
                + "startDay=([-0-9]*),"
                + "startDayOfWeek=([0-9]*),"
                + "startTime=([0-9]*),"
                + "startTimeMode=([0-9]*),"
                + "endMode=([0-9]*),"
                + "endMonth=([0-9]*),"
                + "endDay=([-0-9]*),"
                + "endDayOfWeek=([0-9]*),"
                + "endTime=([0-9]*),"
                + "endTimeMode=([0-9]*).*";
        Pattern pattern = Pattern.compile( patternString );
        String tzString = tz.toString();
        Matcher matcher = pattern.matcher( tzString );
        if ( !matcher.matches() ) {
            throw new AssertionError( "tz.toString not of expected format: " + tzString );
        }
        int j = 0;
        int startMode = Integer.parseInt( matcher.group( ++j ) );
        int startMonth = Integer.parseInt( matcher.group( ++j ) );
        int startDay = Integer.parseInt( matcher.group( ++j ) );
        int startDayOfWeek = Integer.parseInt( matcher.group( ++j ) );
        int startTime = Integer.parseInt( matcher.group( ++j ) );
        int startTimeMode = Integer.parseInt( matcher.group( ++j ) );
        int endMode = Integer.parseInt( matcher.group( ++j ) );
        int endMonth = Integer.parseInt( matcher.group( ++j ) );
        int endDay = Integer.parseInt( matcher.group( ++j ) );
        int endDayOfWeek = Integer.parseInt( matcher.group( ++j ) );
        int endTime = Integer.parseInt( matcher.group( ++j ) );
        int endTimeMode = Integer.parseInt( matcher.group( ++j ) );
        appendPosixDaylightTransition(
                tz,
                buf,
                startMode,
                startDay,
                startMonth,
                startDayOfWeek,
                startTime,
                startTimeMode,
                verbose,
                false );
        appendPosixDaylightTransition(
                tz,
                buf,
                endMode,
                endDay,
                endMonth,
                endDayOfWeek,
                endTime,
                endTimeMode,
                verbose,
                true );
        return buf.toString();
    }


    /**
     * Writes a daylight savings time transition to a POSIX timezone description.
     *
     * @param tz Timezone
     * @param buf Buffer to append to
     * @param mode Transition mode
     * @param day Day of transition
     * @param month Month of transition
     * @param dayOfWeek Day of week of transition
     * @param time Time of transition in millis
     * @param timeMode Mode of time transition
     * @param verbose Verbose
     * @param isEnd Whether this transition is leaving DST
     */
    private static void appendPosixDaylightTransition(
            TimeZone tz,
            StringBuilder buf,
            int mode,
            int day,
            int month,
            int dayOfWeek,
            int time,
            int timeMode,
            boolean verbose,
            boolean isEnd ) {
        buf.append( ',' );
        int week = day;
        switch ( mode ) {
            case 1: // SimpleTimeZone.DOM_MODE
                throw Util.needToImplement( 0 );

            case 3: // SimpleTimeZone.DOW_GE_DOM_MODE

                // If the day is 1, 8, 15, 22, we can translate this to case 2.
                week = switch ( day ) {
                    case 1 -> 1; // 1st week of month
                    case 8 -> 2; // 2nd week of month
                    case 15 -> 3; // 3rd week of month
                    case 22 -> 4; // 4th week of month
                    default -> throw new AssertionError( "POSIX timezone format cannot represent " + tz );
                };
                // fall through

            case 2: // SimpleTimeZone.DOW_IN_MONTH_MODE
                buf.append( 'M' );
                buf.append( month + 1 ); // 1 <= m <= 12
                buf.append( '.' );
                if ( week == -1 ) {
                    // java represents 'last week' differently from POSIX
                    week = 5;
                }
                buf.append( week ); // 1 <= n <= 5, 5 means 'last'
                buf.append( '.' );
                buf.append( dayOfWeek - 1 ); // 0 <= d <= 6, 0=Sunday
                break;

            case 4: // SimpleTimeZone.DOW_LE_DOM_MODE
                throw Util.needToImplement( 0 );
            default:
                throw new AssertionError( "unexpected value: " + mode );
        }
        switch ( timeMode ) {
            case 0: // SimpleTimeZone.WALL_TIME
                break;
            case 1: // SimpleTimeZone.STANDARD_TIME, e.g. Australia/Sydney
                if ( isEnd ) {
                    time += tz.getDSTSavings();
                }
                break;
            case 2: // SimpleTimeZone.UTC_TIME, e.g. Europe/Paris
                time += tz.getRawOffset();
                if ( isEnd ) {
                    time += tz.getDSTSavings();
                }
                break;
        }
        if ( verbose || (time != 7200000) ) {
            // POSIX allows us to omit the time if it is 2am (the default)
            buf.append( '/' );
            appendPosixTime( buf, time );
        }
    }


    /**
     * Given a time expressed in milliseconds, append the time formatted as "hh[:mm[:ss]]".
     *
     * @param buf Buffer to append to
     * @param millis Milliseconds
     */
    private static void appendPosixTime( StringBuilder buf, int millis ) {
        if ( millis < 0 ) {
            buf.append( '-' );
            millis = -millis;
        }
        int hours = millis / 3600000;
        buf.append( hours );
        millis -= hours * 3600000;
        if ( millis == 0 ) {
            return;
        }
        buf.append( ':' );
        int minutes = millis / 60000;
        if ( minutes < 10 ) {
            buf.append( '0' );
        }
        buf.append( minutes );
        millis -= minutes * 60000;
        if ( millis == 0 ) {
            return;
        }
        buf.append( ':' );
        int seconds = millis / 1000;
        if ( seconds < 10 ) {
            buf.append( '0' );
        }
        buf.append( seconds );
    }


    /**
     * Parses a locale string.
     * <p>
     * The inverse operation of {@link java.util.Locale#toString()}.
     *
     * @param localeString Locale string, e.g. "en" or "en_US"
     * @return Java locale object
     */
    public static Locale parseLocale( String localeString ) {
        String[] strings = localeString.split( "_" );
        return switch ( strings.length ) {
            case 1 -> new Locale( strings[0] );
            case 2 -> new Locale( strings[0], strings[1] );
            case 3 -> new Locale( strings[0], strings[1], strings[2] );
            default -> throw new AssertionError( "bad locale string '" + localeString + "'" );
        };
    }


    /**
     * Converts a list whose members are automatically down-cast to a given type.
     * <p>
     * If a member of the backing list is not an instanceof <code>E</code>, the accessing method (such as {@link List#get}) will
     * throw a {@link ClassCastException}.
     * <p>
     * All modifications are automatically written to the backing list. Not synchronized.
     *
     * @param list Backing list.
     * @param clazz Class to cast to.
     * @return A list whose members are of the desired type.
     */
    public static <E> List<E> cast( List<? super E> list, Class<E> clazz ) {
        return new CastingList<>( list, clazz );
    }


    /**
     * Converts a iterator whose members are automatically down-cast to a given type.
     * <p>
     * If a member of the backing iterator is not an instanceof <code>E</code>, {@link Iterator#next()}) will throw a {@link ClassCastException}.
     * <p>
     * All modifications are automatically written to the backing iterator.
     * Not synchronized.
     *
     * @param iter Backing iterator.
     * @param clazz Class to cast to.
     * @return An iterator whose members are of the desired type.
     */
    public static <E> Iterator<E> cast( final Iterator<?> iter, final Class<E> clazz ) {
        return new Iterator<E>() {
            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }


            @Override
            public E next() {
                return clazz.cast( iter.next() );
            }


            @Override
            public void remove() {
                iter.remove();
            }
        };
    }


    /**
     * Converts an {@link Iterable} whose members are automatically down-cast to a given type.
     * <p>
     * All modifications are automatically written to the backing iterator.
     * Not synchronized.
     *
     * @param iterable Backing iterable
     * @param clazz Class to cast to
     * @return An iterable whose members are of the desired type.
     */
    public static <E> Iterable<E> cast( final Iterable<? super E> iterable, final Class<E> clazz ) {
        return () -> cast( iterable.iterator(), clazz );
    }


    /**
     * Makes a collection of untyped elements appear as a list of strictly typed elements, by filtering out those which are not of the correct type.
     * <p>
     * The returned object is an {@link Iterable}, which makes it ideal for use with the 'foreach' construct. For example,
     *
     * <blockquote><code>List&lt;Number&gt; numbers = Arrays.asList(1, 2, 3.14,
     * 4, null, 6E23);<br>
     * for (int myInt : filter(numbers, Integer.class)) {<br>
     * &nbsp;&nbsp;&nbsp;&nbsp;print(i);<br>
     * }</code></blockquote>
     *
     * will print 1, 2, 4.
     *
     * @param iterable Iterable
     * @param includeFilter Class whose instances to include
     */
    public static <E> Iterable<E> filter( final Iterable<?> iterable, final Class<E> includeFilter ) {
        return () -> new Filterator<>( iterable.iterator(), includeFilter );
    }


    public static <E> Collection<E> filter( final Collection<?> collection, final Class<E> includeFilter ) {
        return new AbstractCollection<>() {
            private int size = -1;


            @Override
            public @NotNull Iterator<E> iterator() {
                return new Filterator<>( collection.iterator(), includeFilter );
            }


            @Override
            public int size() {
                if ( size == -1 ) {
                    // Compute size.  This is expensive, but the value collection.size() is not correct since we're
                    // filtering values.  (Some java.util algorithms call next() on the result of iterator() size() times.)
                    int s = 0;
                    for ( E e : this ) {
                        s++;
                    }
                    size = s;
                }

                return size;
            }
        };
    }


    /**
     * Returns a subset of a list containing only elements of a given type.
     * <p>
     * Modifications to the list are NOT written back to the source list.
     *
     * @param list List of objects
     * @param includeFilter Class to filter for
     * @return List of objects of given class (or a subtype)
     */
    public static <E> List<E> filter( final List<?> list, final Class<E> includeFilter ) {
        List<E> result = new ArrayList<>();
        for ( Object o : list ) {
            if ( includeFilter.isInstance( o ) ) {
                result.add( includeFilter.cast( o ) );
            }
        }
        return result;
    }


    /**
     * Returns an exception indicating that we didn't expect to find this enumeration here.
     *
     * @param value Enumeration value which was not expected
     * @return an error, to be thrown
     */
    public static <E extends Enum<E>> Error unexpected( E value ) {
        return new AssertionError( "Was not expecting value '" + value + "' for enumeration '" + value.getDeclaringClass().getName() + "' in this context" );
    }


    /**
     * Creates a map of the values of an enumeration by name.
     *
     * @param clazz Enumeration class
     * @return map of values
     */
    public static <T extends Enum<T>> Map<String, T> enumConstants( Class<T> clazz ) {
        final T[] ts = clazz.getEnumConstants();
        if ( ts == null ) {
            throw new AssertionError( "not an enum type" );
        }
        ImmutableMap.Builder<String, T> builder = ImmutableMap.builder();
        for ( T t : ts ) {
            builder.put( t.name(), t );
        }
        return builder.build();
    }


    /**
     * Returns the value of an enumeration with a particular name.
     * <p>
     * Similar to {@link Enum#valueOf(Class, String)}, but returns {@code null} rather than throwing {@link IllegalArgumentException}.
     *
     * @param clazz Enum class
     * @param name Name of enum constant
     * @param <T> Enum class type
     * @return Enum constant or null
     */
    public static synchronized <T extends Enum<T>> T enumVal( Class<T> clazz, String name ) {
        return clazz.cast( ENUM_CONSTANTS.getUnchecked( clazz ).get( name ) );
    }


    /**
     * Returns the value of an enumeration with a particular or default value if not found.
     *
     * @param default_ Default value (not null)
     * @param name Name of enum constant
     * @param <T> Enum class type
     * @return Enum constant, never null
     */
    public static synchronized <T extends Enum<T>> T enumVal( T default_, String name ) {
        final Class<T> clazz = default_.getDeclaringClass();
        final T t = clazz.cast( ENUM_CONSTANTS.getUnchecked( clazz ).get( name ) );
        if ( t == null ) {
            return default_;
        }
        return t;
    }


    /**
     * Creates a list that returns every {@code n}th element of a list, starting at element {@code k}.
     * <p>
     * It is OK if the list is empty or its size is not a multiple of {@code n}.
     * <p>
     * For instance, {@code quotientList(list, 2, 0)} returns the even elements of a list, and {@code quotientList(list, 2, 1)} returns the odd
     * elements. Those lists are the same length only if list has even size.
     */
    public static <E> List<E> quotientList( final List<E> list, final int n, final int k ) {
        if ( n <= 0 || k < 0 || k >= n ) {
            throw new IllegalArgumentException( "n must be positive; k must be between 0 and n - 1" );
        }
        final int size = (list.size() + n - k - 1) / n;
        return new AbstractList<E>() {
            @Override
            public E get( int index ) {
                return list.get( index * n + k );
            }


            @Override
            public int size() {
                return size;
            }
        };
    }


    /**
     * Given a list with N elements
     * [e<sub>0</sub>, e<sub>1</sub>, ..., e<sub>N-1</sub>]
     * (where N is even), returns a list of the N / 2 elements
     * [ (e<sub>0</sub>, e<sub>1</sub>),
     * (e<sub>2</sub>, e<sub>3</sub>), ... ].
     */
    public static <E extends Comparable<E>> List<Pair<E, E>> pairs( final List<E> list ) {
        return Pair.zip( quotientList( list, 2, 0 ), quotientList( list, 2, 1 ) );
    }


    /**
     * Returns the first value if it is not null, otherwise the second value.
     * <p>
     * The result may be null.
     * <p>
     * Equivalent to the Elvis operator ({@code ?:}) of languages such as Groovy or PHP.
     */
    public static <T> T first( T v0, T v1 ) {
        return v0 != null ? v0 : v1;
    }


    /**
     * Unboxes a {@link Double} value, using a given default value if it is null.
     */
    public static double first( Double v0, double v1 ) {
        return v0 != null ? v0 : v1;
    }


    /**
     * Unboxes a {@link Float} value, using a given default value if it is null.
     */
    public static float first( Float v0, float v1 ) {
        return v0 != null ? v0 : v1;
    }


    /**
     * Unboxes a {@link Integer} value, using a given default value if it is null.
     */
    public static int first( Integer v0, int v1 ) {
        return v0 != null ? v0 : v1;
    }


    /**
     * Unboxes a {@link Long} value, using a given default value if it is null.
     */
    public static long first( Long v0, long v1 ) {
        return v0 != null ? v0 : v1;
    }


    /**
     * Unboxes a {@link Boolean} value, using a given default value if it is null.
     */
    public static boolean first( Boolean v0, boolean v1 ) {
        return v0 != null ? v0 : v1;
    }


    /**
     * Unboxes a {@link Short} value, using a given default value if it is null.
     */
    public static short first( Short v0, short v1 ) {
        return v0 != null ? v0 : v1;
    }


    /**
     * Unboxes a {@link Character} value, using a given default value if it is null.
     */
    public static char first( Character v0, char v1 ) {
        return v0 != null ? v0 : v1;
    }


    /**
     * Unboxes a {@link Byte} value, using a given default value if it is null.
     */
    public static byte first( Byte v0, byte v1 ) {
        return v0 != null ? v0 : v1;
    }


    public static <T> Iterable<T> orEmpty( Iterable<T> v0 ) {
        return v0 != null ? v0 : ImmutableList.of();
    }


    /**
     * Returns the last element of a list.
     *
     * @throws java.lang.IndexOutOfBoundsException if the list is empty
     */
    public static <E> E last( List<E> list ) {
        return list.get( list.size() - 1 );
    }


    /**
     * Returns every element of a list but its last element.
     */
    public static <E> List<E> skipLast( List<E> list ) {
        return skipLast( list, 1 );
    }


    /**
     * Returns every element of a list but its last {@code n} elements.
     */
    public static <E> List<E> skipLast( List<E> list, int n ) {
        return list.subList( 0, list.size() - n );
    }


    /**
     * Returns the last {@code n} elements of a list.
     */
    public static <E> List<E> last( List<E> list, int n ) {
        return list.subList( list.size() - n, list.size() );
    }


    /**
     * Returns all but the first element of a list.
     */
    public static <E> List<E> skip( List<E> list ) {
        return skip( list, 1 );
    }


    /**
     * Returns all but the first {@code n} elements of a list.
     */
    public static <E> List<E> skip( List<E> list, int fromIndex ) {
        return fromIndex == 0 ? list : list.subList( fromIndex, list.size() );
    }


    public static List<Integer> range( final int end ) {
        return new AbstractList<Integer>() {
            @Override
            public int size() {
                return end;
            }


            @Override
            public Integer get( int index ) {
                return index;
            }
        };
    }


    public static List<Integer> range( final int start, final int end ) {
        return new AbstractList<Integer>() {
            @Override
            public int size() {
                return end - start;
            }


            @Override
            public Integer get( int index ) {
                return start + index;
            }
        };
    }


    /**
     * Returns whether the elements of {@code list} are distinct.
     */
    public static <E> boolean isDistinct( List<E> list ) {
        return firstDuplicate( list ) < 0;
    }


    /**
     * Returns the ordinal of the first element in the list which is equal to a previous element in the list.
     * <p>
     * For example,
     * <code>firstDuplicate(Arrays.asList("a", "b", "c", "b", "a"))</code>
     * returns 3, the ordinal of the 2nd "b".
     *
     * @param list List
     * @return Ordinal of first duplicate, or -1 if not found
     */
    public static <E> int firstDuplicate( List<E> list ) {
        final int size = list.size();
        if ( size < 2 ) {
            // Lists of size 0 and 1 are always distinct.
            return -1;
        }
        if ( size < 15 ) {
            // For smaller lists, avoid the overhead of creating a set. Threshold
            // determined empirically using UtilTest.testIsDistinctBenchmark.
            for ( int i = 1; i < size; i++ ) {
                E e = list.get( i );
                for ( int j = i - 1; j >= 0; j-- ) {
                    E e1 = list.get( j );
                    if ( Objects.equals( e, e1 ) ) {
                        return i;
                    }
                }
            }
            return -1;
        }
        final Map<E, Object> set = new HashMap<>( size );
        for ( E e : list ) {
            if ( set.put( e, "" ) != null ) {
                return set.size();
            }
        }
        return -1;
    }


    /**
     * Converts a list into a list with unique elements.
     * <p>
     * The order is preserved; the second and subsequent occurrences are removed.
     * <p>
     * If the list is already unique it is returned unchanged.
     */
    public static <E> List<E> distinctList( List<E> list ) {
        if ( isDistinct( list ) ) {
            return list;
        }
        return ImmutableList.copyOf( new LinkedHashSet<>( list ) );
    }


    /**
     * Converts an iterable into a list with unique elements.
     * <p>
     * The order is preserved; the second and subsequent occurrences are removed.
     * <p>
     * If {@code iterable} is a unique list it is returned unchanged.
     */
    public static <E> List<E> distinctList( Iterable<E> keys ) {
        if ( keys instanceof Set ) {
            return ImmutableList.copyOf( keys );
        }
        if ( keys instanceof List ) {
            @SuppressWarnings("unchecked") final List<E> list = (List) keys;
            if ( isDistinct( list ) ) {
                return list;
            }
        }
        return ImmutableList.copyOf( Sets.newLinkedHashSet( keys ) );
    }


    /**
     * Returns whether two collections have any elements in common.
     */
    public static <E> boolean intersects( Collection<E> c0, Collection<E> c1 ) {
        for ( E e : c1 ) {
            if ( c0.contains( e ) ) {
                return true;
            }
        }
        return false;
    }


    /**
     * Looks for a string within a list of strings, using a given case-sensitivity policy, and returns the position at which the first match
     * is found, or -1 if there are no matches.
     */
    public static int findMatch( List<String> strings, String seek, boolean caseSensitive ) {
        if ( caseSensitive ) {
            return strings.indexOf( seek );
        }
        for ( int i = 0; i < strings.size(); i++ ) {
            String s = strings.get( i );
            if ( s.equalsIgnoreCase( seek ) ) {
                return i;
            }
        }
        return -1;
    }


    /**
     * Returns whether a name matches another according to a given case-sensitivity policy.
     */
    public static boolean matches( boolean caseSensitive, String s0, String s1 ) {
        return caseSensitive ? s1.equals( s0 ) : s1.equalsIgnoreCase( s0 );
    }


    /**
     * Returns whether one list is a prefix of another.
     */
    public static <E> boolean startsWith( List<E> list0, List<E> list1 ) {
        if ( list0 == list1 ) {
            return true;
        }
        final int size = list1.size();
        if ( list0.size() < size ) {
            return false;
        }
        for ( int i = 0; i < size; i++ ) {
            if ( !Objects.equals( list0.get( i ), list1.get( i ) ) ) {
                return false;
            }
        }
        return true;
    }


    /**
     * Converts ["ab", "c"] to "ab"."c".
     */
    public static String listToString( List<String> list ) {
        final StringBuilder b = new StringBuilder();
        for ( String s : list ) {
            if ( b.length() > 0 ) {
                b.append( "." );
            }
            b.append( '"' );
            b.append( s.replace( "\"", "\"\"" ) );
            b.append( '"' );
        }
        return b.toString();
    }


    public static List<String> stringToList( String s ) {
        if ( s.isEmpty() ) {
            return ImmutableList.of();
        }
        final ImmutableList.Builder<String> builder = ImmutableList.builder();
        final StringBuilder b = new StringBuilder();
        int i = 0;
        //noinspection ConditionalBreakInInfiniteLoop
        for ( ; ; ) {
            char c = s.charAt( i );
            if ( c != '"' ) {
                throw new IllegalArgumentException();
            }
            for ( ; ; ) {
                c = s.charAt( ++i );
                if ( c == '"' ) {
                    if ( i == s.length() - 1 ) {
                        break;
                    }
                    ++i;
                    c = s.charAt( i );
                    if ( c == '.' ) {
                        break;
                    }
                    if ( c != '"' ) {
                        throw new IllegalArgumentException();
                    }
                }
                b.append( c );
            }
            builder.add( b.toString() );
            b.setLength( 0 );
            if ( ++i >= s.length() ) {
                break;
            }
        }
        return builder.build();
    }


    /**
     * Converts a number into human-readable form, with 3 digits and a "K", "M" or "G" multiplier for thousands, millions or billions.
     * <p>
     * Examples: -2, 0, 1, 999, 1.00K, 1.99K, 3.45M, 4.56B.
     */
    public static String human( double d ) {
        if ( d == 0d ) {
            return "0";
        }
        if ( d < 0d ) {
            return "-" + human( -d );
        }
        final int digitCount = (int) Math.floor( Math.log10( d ) );
        return switch ( digitCount ) {
            case 0, 1, 2 -> Integer.toString( (int) d );
            case 3, 4, 5 -> digits3( Math.round( d / 10D ), digitCount % 3 ) + "K";
            case 6, 7, 8 -> digits3( Math.round( d / 10000D ), digitCount % 3 ) + "M";
            case 9, 10, 11 -> digits3( Math.round( d / 10000000D ), digitCount % 3 ) + "G";
            default -> Double.toString( d );
        };
    }


    private static String digits3( long x, int z ) {
        final String s = Long.toString( x );
        return switch ( z ) {
            case 0 -> s.charAt( 0 ) + "." + s.substring( 1, 3 );
            case 1 -> s.substring( 0, 2 ) + "." + s.substring( 2, 3 );
            default -> s.substring( 0, 3 );
        };
    }


    /**
     * Prints the given code with line numbering.
     */
    public static void debugCode( PrintStream out, String code ) {
        out.println();
        StringReader sr = new StringReader( code );
        BufferedReader br = new BufferedReader( sr );
        try {
            String line;
            for ( int i = 1; (line = br.readLine()) != null; i++ ) {
                out.printf( "/*%-4d */", i );
                out.println( line );
            }
        } catch ( IOException e ) {
            // not possible
        }
    }


    /**
     * Returns the value of a system property as a boolean.
     * <p>
     * For example, the property "foo" is considered true if you supply {@code -Dfoo} or {@code -Dfoo=true} or {@code -Dfoo=TRUE},
     * false if you omit the flag or supply {@code -Dfoo=false}.
     *
     * @param property Property name
     * @return Whether property is true
     */
    public static boolean getBooleanProperty( String property ) {
        return getBooleanProperty( property, false );
    }


    /**
     * Returns the value of a system property as a boolean, returning a given default value if the property is not specified.
     */
    public static boolean getBooleanProperty( String property, boolean defaultValue ) {
        final String v = System.getProperties().getProperty( property );
        if ( v == null ) {
            return defaultValue;
        }
        return v.isEmpty() || "true".equalsIgnoreCase( v );
    }


    /**
     * Returns a copy of a list of lists, making the component lists immutable if they are not already.
     */
    public static <E> List<List<E>> immutableCopy( Iterable<? extends Iterable<E>> lists ) {
        int n = 0;
        for ( Iterable<E> list : lists ) {
            if ( !(list instanceof ImmutableList) ) {
                ++n;
            }
        }
        if ( n == 0 ) {
            // Lists are already immutable. Furthermore, if the outer list is immutable we will just return "lists" unchanged.
            //noinspection unchecked
            return ImmutableList.copyOf( (Iterable<List<E>>) lists );
        }
        final ImmutableList.Builder<List<E>> builder = ImmutableList.builder();
        for ( Iterable<E> list : lists ) {
            builder.add( ImmutableList.copyOf( list ) );
        }
        return builder.build();
    }


    /**
     * Creates a {@link PrintWriter} to a given output stream using UTF-8 character set.
     * <p>
     * Does not use the default character set.
     */
    public static PrintWriter printWriter( OutputStream out ) {
        return new PrintWriter( new BufferedWriter( new OutputStreamWriter( out, StandardCharsets.UTF_8 ) ) );
    }


    /**
     * Creates a {@link PrintWriter} to a given file using UTF-8 character set.
     * <p>
     * Does not use the default character set.
     */
    public static PrintWriter printWriter( File file ) throws FileNotFoundException {
        return printWriter( new FileOutputStream( file ) );
    }


    /**
     * Creates a {@link BufferedReader} to a given input stream using UTF-8 character set.
     * <p>
     * Does not use the default character set.
     */
    public static BufferedReader reader( InputStream in ) {
        return new BufferedReader( new InputStreamReader( in, StandardCharsets.UTF_8 ) );
    }


    /**
     * Creates a {@link BufferedReader} to read a given file using UTF-8 character set.
     * <p>
     * Does not use the default character set.
     */
    public static BufferedReader reader( File file ) throws FileNotFoundException {
        return reader( new FileInputStream( file ) );
    }


    /**
     * Creates a {@link Calendar} in the UTC time zone and root locale.
     * Does not use the time zone or locale.
     */
    public static Calendar calendar() {
        return Calendar.getInstance( DateTimeUtils.UTC_ZONE, Locale.ROOT );
    }


    /**
     * Creates a {@link Calendar} in the UTC time zone and root locale with a given time.
     */
    public static Calendar calendar( long millis ) {
        Calendar calendar = calendar();
        calendar.setTimeInMillis( millis );
        return calendar;
    }


    /**
     * Returns a {@code Collector} that accumulates the input elements into a Guava {@code ImmutableList} via a {@code ImmutableList.Builder}.
     *
     * @param <T> Type of the input elements
     * @return a {@code Collector} that collects all the input elements into an {@link ImmutableList}, in encounter order
     */
    public static <T> Collector<T, ImmutableList.Builder<T>, ImmutableList<T>>
    toImmutableList() {
        return Collector.of( ImmutableList::builder, ImmutableList.Builder::add,
                ( t, u ) -> {
                    t.addAll( u.build() );
                    return t;
                },
                ImmutableList.Builder::build );
    }


    /**
     * Transforms a list, applying a function to each element.
     */
    public static <F, T> List<T> transform( List<F> list, java.util.function.Function<F, T> function ) {
        if ( list instanceof RandomAccess ) {
            return new RandomAccessTransformingList<>( list, function );
        } else {
            return new TransformingList<>( list, function );
        }
    }


    /**
     * Filters an iterable.
     */
    public static <E> Iterable<E> filter( Iterable<E> iterable, Predicate<E> predicate ) {
        return () -> filter( iterable.iterator(), predicate );
    }


    /**
     * Checks to see if a specific port is available.
     * Taken from: https://stackoverflow.com/a/435579
     *
     * @param port the port to check for availability
     */
    public static boolean checkIfPortIsAvailable( int port ) {
        if ( port < 1 || port > 65535 ) {
            throw new IllegalArgumentException( "Invalid port number: " + port );
        }

        ServerSocket ss = null;
        DatagramSocket ds = null;
        try {
            ss = new ServerSocket( port );
            ss.setReuseAddress( true );
            ds = new DatagramSocket( port );
            ds.setReuseAddress( true );
            return true;
        } catch ( IOException ignored ) {
        } finally {
            if ( ds != null ) {
                ds.close();
            }

            if ( ss != null ) {
                try {
                    ss.close();
                } catch ( IOException e ) {
                    /* should not be thrown */
                }
            }
        }

        return false;
    }


    public static void closeNoThrow( Closeable c ) {
        if ( c != null ) {
            try {
                c.close();
            } catch ( IOException ignore ) {
            }
        }
    }


    /**
     * Filters an iterator.
     */
    public static <E> Iterator<E> filter( Iterator<E> iterator, Predicate<E> predicate ) {
        return new FilteringIterator<>( iterator, predicate );
    }


    public static Monotonicity getMonotonicity( Entity entity, String columnName ) {
        if ( entity.dataModel != DataModel.RELATIONAL ) {
            return Monotonicity.NOT_MONOTONIC;
        }

        for ( AlgCollation collation : entity.getCollations() ) {
            final AlgFieldCollation fieldCollation = collation.getFieldCollations().get( 0 );
            final int fieldIndex = fieldCollation.getFieldIndex();
            if ( fieldIndex < entity.getTupleType().getFieldCount() && entity.getTupleType().getFieldNames().get( fieldIndex ).equals( columnName ) ) {
                return fieldCollation.direction.monotonicity();
            }
        }
        return Monotonicity.NOT_MONOTONIC;
    }


    public static Collation getDefaultCollation() {
        return Collation.COERCIBLE;
    }


    public static boolean containsEntity( AlgNode a ) {
        return a.containsEntity();
    }


    /**
     * Exception used to interrupt a tree walk of any kind.
     */
    public static class FoundOne extends ControlFlowException {

        private final Object node;

        /**
         * Singleton instance. Can be used if you don't care about node.
         */
        public static final FoundOne NULL = new FoundOne( null );


        public FoundOne( Object node ) {
            this.node = node;
        }


        public Object getNode() {
            return node;
        }

    }


    /**
     * Visitor which looks for an OVER clause inside a tree of {@link Node} objects.
     */
    public static class OverFinder extends BasicNodeVisitor<Void> {

        public static final OverFinder INSTANCE = new Util.OverFinder();


        @Override
        public Void visit( Call call ) {
            if ( call.getKind() == Kind.OVER ) {
                throw FoundOne.NULL;
            }
            return super.visit( call );
        }

    }


    /**
     * List that returns the same number of elements as a backing list, applying a transformation function to each one.
     *
     * @param <F> Element type of backing list
     * @param <T> Element type of this list
     */
    private static class TransformingList<F, T> extends AbstractList<T> {

        private final java.util.function.Function<F, T> function;
        private final List<F> list;


        TransformingList( List<F> list, java.util.function.Function<F, T> function ) {
            this.function = function;
            this.list = list;
        }


        @Override
        public T get( int i ) {
            return function.apply( list.get( i ) );
        }


        @Override
        public int size() {
            return list.size();
        }


        @Override
        @Nonnull
        public Iterator<T> iterator() {
            return listIterator();
        }

    }


    /**
     * Extension to {@link TransformingList} that implements {@link RandomAccess}.
     *
     * @param <F> Element type of backing list
     * @param <T> Element type of this list
     */
    private static class RandomAccessTransformingList<F, T> extends TransformingList<F, T> implements RandomAccess {

        RandomAccessTransformingList( List<F> list, java.util.function.Function<F, T> function ) {
            super( list, function );
        }

    }


    /**
     * Iterator that applies a predicate to each element.
     *
     * @param <T> Element type
     */
    private static class FilteringIterator<T> implements Iterator<T> {

        private static final Object DUMMY = new Object();
        final Iterator<? extends T> iterator;
        private final Predicate<T> predicate;
        T current;


        FilteringIterator( Iterator<? extends T> iterator, Predicate<T> predicate ) {
            this.iterator = iterator;
            this.predicate = predicate;
            current = moveNext();
        }


        @Override
        public boolean hasNext() {
            return current != DUMMY;
        }


        @Override
        public T next() {
            final T t = this.current;
            current = moveNext();
            return t;
        }


        protected T moveNext() {
            while ( iterator.hasNext() ) {
                T t = iterator.next();
                if ( predicate.test( t ) ) {
                    return t;
                }
            }
            return (T) DUMMY;
        }

    }

}
