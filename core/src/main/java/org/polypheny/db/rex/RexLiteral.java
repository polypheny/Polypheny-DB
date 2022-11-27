/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.rex;


import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.TimeUnit;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.runtime.PolyCollections.PolyList;
import org.polypheny.db.runtime.PolyCollections.PolyMap;
import org.polypheny.db.schema.graph.PolyEdge;
import org.polypheny.db.schema.graph.PolyGraph;
import org.polypheny.db.schema.graph.PolyNode;
import org.polypheny.db.schema.graph.PolyPath;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Collation;
import org.polypheny.db.util.CompositeList;
import org.polypheny.db.util.DateString;
import org.polypheny.db.util.Litmus;
import org.polypheny.db.util.NlsString;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.SaffronProperties;
import org.polypheny.db.util.TimeString;
import org.polypheny.db.util.TimestampString;
import org.polypheny.db.util.Unsafe;
import org.polypheny.db.util.Util;


/**
 * Constant value in a row-expression.
 *
 * There are several methods for creating literals in {@link RexBuilder}: {@link RexBuilder#makeLiteral(boolean)} and so forth.
 *
 * How is the value stored? In that respect, the class is somewhat of a black box. There is a {@link #getValue} method which returns the value as an object, but the type of that value is implementation detail,
 * and it is best that your code does not depend upon that knowledge. It is better to use task-oriented methods such as {@link #getValue2} and {@link #toJavaString}.
 *
 * The allowable types and combinations are:
 *
 * <table>
 * <caption>Allowable types for RexLiteral instances</caption>
 * <tr>
 * <th>TypeName</th>
 * <th>Meaning</th>
 * <th>Value type</th>
 * </tr>
 * <tr>
 * <td>{@link PolyType#NULL}</td>
 * <td>The null value. It has its own special type.</td>
 * <td>null</td>
 * </tr>
 * <tr>
 * <td>{@link PolyType#BOOLEAN}</td>
 * <td>Boolean, namely <code>TRUE</code>, <code>FALSE</code> or <code> UNKNOWN</code>.</td>
 * <td>{@link Boolean}, or null represents the UNKNOWN value</td>
 * </tr>
 * <tr>
 * <td>{@link PolyType#DECIMAL}</td>
 * <td>Exact number, for example <code>0</code>, <code>-.5</code>, <code> 12345</code>.</td>
 * <td>{@link BigDecimal}</td>
 * </tr>
 * <tr>
 * <td>{@link PolyType#DOUBLE}</td>
 * <td>Approximate number, for example <code>6.023E-23</code>.</td>
 * <td>{@link BigDecimal}</td>
 * </tr>
 * <tr>
 * <td>{@link PolyType#DATE}</td>
 * <td>Date, for example <code>DATE '1969-04'29'</code></td>
 * <td>{@link Calendar}; also {@link Calendar} (UTC time zone) and {@link Integer} (days since POSIX epoch)</td>
 * </tr>
 * <tr>
 * <td>{@link PolyType#TIME}</td>
 * <td>Time, for example <code>TIME '18:37:42.567'</code></td>
 * <td>{@link Calendar}; also {@link Calendar} (UTC time zone) and {@link Integer} (milliseconds since midnight)</td>
 * </tr>
 * <tr>
 * <td>{@link PolyType#TIMESTAMP}</td>
 * <td>Timestamp, for example <code>TIMESTAMP '1969-04-29 18:37:42.567'</code></td>
 * <td>{@link TimestampString}; also {@link Calendar} (UTC time zone) and {@link Long} (milliseconds since POSIX epoch)</td>
 * </tr>
 * <tr>
 * <td>{@link PolyType#INTERVAL_DAY},
 * {@link PolyType#INTERVAL_DAY_HOUR},
 * {@link PolyType#INTERVAL_DAY_MINUTE},
 * {@link PolyType#INTERVAL_DAY_SECOND},
 * {@link PolyType#INTERVAL_HOUR},
 * {@link PolyType#INTERVAL_HOUR_MINUTE},
 * {@link PolyType#INTERVAL_HOUR_SECOND},
 * {@link PolyType#INTERVAL_MINUTE},
 * {@link PolyType#INTERVAL_MINUTE_SECOND},
 * {@link PolyType#INTERVAL_SECOND}</td>
 * <td>Interval, for example <code>INTERVAL '4:3:2' HOUR TO SECOND</code></td>
 * <td>{@link BigDecimal}; also {@link Long} (milliseconds)</td>
 * </tr>
 * <tr>
 * <td>{@link PolyType#INTERVAL_YEAR}, {@link PolyType#INTERVAL_YEAR_MONTH}, {@link PolyType#INTERVAL_MONTH}</td>
 * <td>Interval, for example <code>INTERVAL '2-3' YEAR TO MONTH</code></td>
 * <td>{@link BigDecimal}; also {@link Integer} (months)</td>
 * </tr>
 * <tr>
 * <td>{@link PolyType#CHAR}</td>
 * <td>Character constant, for example <code>'Hello, world!'</code>, <code>''</code>, <code>_N'Bonjour'</code>, <code>_ISO-8859-1'It''s superman!' COLLATE SHIFT_JIS$ja_JP$2</code>. These are always CHAR, never VARCHAR.</td>
 * <td>{@link NlsString}; also {@link String}</td>
 * </tr>
 * <tr>
 * <td>{@link PolyType#BINARY}</td>
 * <td>Binary constant, for example <code>X'7F34'</code>. (The number of hexits must be even; see above.) These constants are always BINARY, never VARBINARY.</td>
 * <td>{@link ByteBuffer}; also {@code byte[]}</td>
 * </tr>
 * <tr>
 * <td>{@link PolyType#SYMBOL}</td>
 * <td>A symbol is a special type used to make parsing easier; it is not part of the SQL standard, and is not exposed to end-users. It is used to hold a flag, such as the LEADING flag in a call to the function <code> TRIM([LEADING|TRAILING|BOTH] chars FROM string)</code>.</td>
 * <td>An enum class</td>
 * </tr>
 * </table>
 */
public class RexLiteral extends RexNode implements Comparable<RexLiteral> {

    private static final Gson gson = new Gson();

    /**
     * The value of this literal. Must be consistent with its type, as per {@link #valueMatchesType}. For example, you can't store an {@link Integer} value here just because you feel like it -- all numbers are
     * represented by a {@link BigDecimal}. But since this field is private, it doesn't really matter how the values are stored.
     */
    private final Comparable value;

    /**
     * The real type of this literal, as reported by {@link #getType}.
     */
    private final AlgDataType type;

    // TODO jvs: Use SqlTypeFamily instead; it exists for exactly this purpose (to avoid the confusion which results from overloading PolyType).
    /**
     * An indication of the broad type of this literal -- even if its type isn't a SQL type. Sometimes this will be different than the SQL type; for example, all exact numbers, including integers have typeName
     * {@link PolyType#DECIMAL}. See {@link #valueMatchesType} for the definitive story.
     */
    private final PolyType typeName;

    private static final ImmutableList<TimeUnit> TIME_UNITS = ImmutableList.copyOf( TimeUnit.values() );


    /**
     * Creates a <code>RexLiteral</code>.
     */
    public RexLiteral( Comparable value, AlgDataType type, PolyType typeName ) {
        this.value = value;
        this.type = Objects.requireNonNull( type );
        this.typeName = Objects.requireNonNull( typeName );
        if ( !valueMatchesType( value, typeName, true ) ) {
            System.err.println( value );
            System.err.println( value.getClass().getCanonicalName() );
            System.err.println( type );
            System.err.println( typeName );
            throw new IllegalArgumentException();
        }
//        Preconditions.checkArgument( valueMatchesType( value, typeName, true ) );
        Preconditions.checkArgument( (value != null) || type.isNullable() );
        Preconditions.checkArgument( typeName != PolyType.ANY );
        this.digest = computeDigest( RexDigestIncludeType.OPTIONAL );
    }


    public RexLiteral( Comparable value, AlgDataType type, PolyType typeName, boolean raw ) {
        this.value = value;
        this.type = Objects.requireNonNull( type );
        this.typeName = Objects.requireNonNull( typeName );
        this.digest = computeDigest( RexDigestIncludeType.OPTIONAL );
    }


    /**
     * Returns a string which concisely describes the definition of this rex literal. Two literals are equivalent if and only if their digests are the same.
     *
     * The digest does not contain the expression's identity, but does include the identity of children.
     *
     * Technically speaking 1:INT differs from 1:FLOAT, so we need data type in the literal's digest, however we want to avoid extra verbosity of the {@link AlgNode#getDigest()} for readability purposes, so we omit type info in certain cases.
     * For instance, 1:INT becomes 1 (INT is implied by default), however 1:BIGINT always holds the type
     *
     * Here's a non-exhaustive list of the "well known cases":
     * <ul>
     * <li>Hide "NOT NULL" for not null literals</li>
     * <li>Hide INTEGER, BOOLEAN, SYMBOL, TIME(0), TIMESTAMP(0), DATE(0) types</li>
     * <li>Hide collation when it matches IMPLICIT/COERCIBLE</li>
     * <li>Hide charset when it matches default</li>
     * <li>Hide CHAR(xx) when literal length is equal to the precision of the type. In other words, use 'Bob' instead of 'Bob':CHAR(3)</li>
     * <li>Hide BOOL for AND/OR arguments. In other words, AND(true, null) means null is BOOL.</li>
     * <li>Hide types for literals in simple binary operations (e.g. +, -, *, /, comparison) when type of the other argument is clear. See {@link RexCall#computeDigest(boolean)} For instance: =(true. null) means null is BOOL. =($0, null) means the type of null matches the type of $0.</li>
     * </ul>
     *
     * @param includeType whether the digest should include type or not
     * @return digest
     */
    public final String computeDigest( RexDigestIncludeType includeType ) {
        if ( includeType == RexDigestIncludeType.OPTIONAL ) {
            if ( digest != null ) {
                // digest is initialized with OPTIONAL, so cached value matches for includeType=OPTIONAL as well
                return digest;
            }
            // Compute we should include the type or not
            includeType = digestIncludesType();
        } else if ( digest != null && includeType == digestIncludesType() ) {
            // The digest is always computed with includeType=OPTIONAL
            // If it happened to omit the type, we want to optimize computeDigest(NO_TYPE) as well
            // If the digest includes the type, we want to optimize computeDigest(ALWAYS)
            return digest;
        }

        return toJavaString( value, typeName, type, includeType );
    }


    /**
     * Returns true if {@link RexDigestIncludeType#OPTIONAL} digest would include data type.
     *
     * @return true if {@link RexDigestIncludeType#OPTIONAL} digest would include data type
     * @see RexCall#computeDigest(boolean)
     */
    RexDigestIncludeType digestIncludesType() {
        return shouldIncludeType( value, type );
    }


    public static Pair<Comparable, PolyType> convertType( Comparable value, AlgDataType typeName ) {
        switch ( typeName.getPolyType() ) {
            case INTEGER:
            case BIGINT:
            case TINYINT:
            case SMALLINT:
            case DECIMAL:
            case DOUBLE:
            case FLOAT:
            case REAL:
                if ( value instanceof Short ) {
                    return new Pair<>( new BigDecimal( (short) value ), PolyType.DECIMAL );
                } else if ( value instanceof Byte ) {
                    return new Pair<>( new BigDecimal( (byte) value ), PolyType.DECIMAL );
                } else if ( value instanceof Character ) {
                    return new Pair<>( new BigDecimal( (char) value ), PolyType.DECIMAL );
                } else if ( value instanceof Integer ) {
                    return new Pair<>( new BigDecimal( (int) value ), PolyType.DECIMAL );
                } else if ( value instanceof Long ) {
                    return new Pair<>( new BigDecimal( (long) value ), PolyType.DECIMAL );
                } else if ( value instanceof Float ) {
                    return new Pair<>( new BigDecimal( (float) value ), PolyType.DECIMAL );
                } else if ( value instanceof Double ) {
                    return new Pair<>( new BigDecimal( (double) value ), PolyType.DECIMAL );
                }
            case VARCHAR:
            case CHAR:
                if ( value instanceof String ) {
                    return new Pair<>( new NlsString( (String) value, typeName.getCharset().name(), typeName.getCollation() ), PolyType.CHAR );
                }
            case TIMESTAMP:
                if ( value instanceof String ) {
                    return new Pair<>( new TimestampString( (String) value ), PolyType.TIMESTAMP );
                } else if ( value instanceof LocalDateTime ) {
                    final LocalDateTime dt = (LocalDateTime) value;
                    final TimestampString ts = new TimestampString(
                            dt.getYear(),
                            dt.getMonthValue(),
                            dt.getDayOfMonth(),
                            dt.getHour(),
                            dt.getMinute(),
                            dt.getSecond()
                    );
                    return new Pair<>( ts, PolyType.TIMESTAMP );
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if ( value instanceof String ) {
                    return new Pair<>( new TimestampString( (String) value ), PolyType.TIMESTAMP_WITH_LOCAL_TIME_ZONE );
                } else if ( value instanceof LocalDateTime ) {
                    final LocalDateTime dt = (LocalDateTime) value;
                    final TimestampString ts = new TimestampString(
                            dt.getYear(),
                            dt.getMonthValue(),
                            dt.getDayOfMonth(),
                            dt.getHour(),
                            dt.getMinute(),
                            dt.getSecond()
                    );
                    return new Pair<>( ts, PolyType.TIMESTAMP_WITH_LOCAL_TIME_ZONE );
                }
        }
        return new Pair<>( value, typeName.getPolyType() );
    }


    /**
     * @return whether value is appropriate for its type (we have rules about these things)
     */
    public static boolean valueMatchesType( Comparable value, PolyType typeName, boolean strict ) {
        if ( value == null ) {
            return true;
        }
        switch ( typeName ) {
            case BOOLEAN:
                // Unlike SqlLiteral, we do not allow boolean null.
                return value instanceof Boolean;
            case NULL:
                return false; // value should have been null
            case INTEGER: // not allowed -- use Decimal
            case TINYINT:
            case SMALLINT:
                if ( strict ) {
                    throw Util.unexpected( typeName );
                }
                // fall through
            case DECIMAL:
            case DOUBLE:
            case FLOAT:
            case REAL:
            case BIGINT:
                return value instanceof BigDecimal;
            case DATE:
                return value instanceof DateString;
            case TIME:
                return value instanceof TimeString;
            case TIME_WITH_LOCAL_TIME_ZONE:
                return value instanceof TimeString;
            case TIMESTAMP:
                return value instanceof TimestampString;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return value instanceof TimestampString;
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                // The value of a DAY-TIME interval (whatever the start and end units, even say HOUR TO MINUTE) is in milliseconds (perhaps fractional milliseconds). The value of a YEAR-MONTH interval is in months.
                return value instanceof BigDecimal;
            case VARBINARY: // not allowed -- use Binary
                if ( strict ) {
                    throw Util.unexpected( typeName );
                }
                // fall through
            case BINARY:
                return value instanceof ByteString;
            case VARCHAR: // not allowed -- use Char
                if ( strict ) {
                    throw Util.unexpected( typeName );
                }
                // fall through
            case CHAR:
                // A SqlLiteral's charset and collation are optional; not so a RexLiteral.
                return (value instanceof NlsString)
                        && (((NlsString) value).getCharset() != null)
                        && (((NlsString) value).getCollation() != null);
            case SYMBOL:
                return value instanceof Enum;
            case ROW:
            case MULTISET:
            case ARRAY:
                return value instanceof PolyList;
            case ANY:
                // Literal of type ANY is not legal. "CAST(2 AS ANY)" remains an integer literal surrounded by a cast function.
                return false;
            case GRAPH:
                return value instanceof PolyGraph;
            case NODE:
                return value instanceof PolyNode;
            case EDGE:
                return value instanceof PolyEdge;
            case PATH:
                return value instanceof PolyPath;
            case MAP:
                return value instanceof Map;
            default:
                throw Util.unexpected( typeName );
        }
    }


    private static String toJavaString( Comparable<?> value, PolyType typeName, AlgDataType type, RexDigestIncludeType includeType ) {
        assert includeType != RexDigestIncludeType.OPTIONAL : "toJavaString must not be called with includeType=OPTIONAL";
        String fullTypeString = type.getFullTypeString();
        if ( value == null ) {
            return includeType == RexDigestIncludeType.NO_TYPE ? "null" : "null:" + fullTypeString;
        }
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter( sw );
        printAsJava( value, pw, typeName, false, includeType );
        pw.flush();

        if ( includeType != RexDigestIncludeType.NO_TYPE ) {
            sw.append( ':' );
            if ( !fullTypeString.endsWith( "NOT NULL" ) ) {
                sw.append( fullTypeString );
            } else {
                // Trim " NOT NULL". Apparently, the literal is not null, so we just print the data type.
                Unsafe.append( sw, fullTypeString, 0, fullTypeString.length() - 9 );
            }
        }
        return sw.toString();
    }


    /**
     * Computes if data type can be omitted from the digset.
     * For instance, {@code 1:BIGINT} has to keep data type while {@code 1:INT} should be represented as just {@code 1}.
     *
     * Implementation assumption: this method should be fast. In fact might call {@link NlsString#getValue()} which could decode the string, however we rely on the cache there.
     *
     * @param value value of the literal
     * @param type type of the literal
     * @return NO_TYPE when type can be omitted, ALWAYS otherwise
     * @see RexLiteral#computeDigest(RexDigestIncludeType)
     */
    private static RexDigestIncludeType shouldIncludeType( Comparable value, AlgDataType type ) {
        if ( type.isNullable() ) {
            // This means "null literal", so we require a type for it
            // There might be exceptions like AND(null, true) which are handled by RexCall#computeDigest
            return RexDigestIncludeType.ALWAYS;
        }
        // The variable here simplifies debugging (one can set a breakpoint at return) final ensures we set the value in all the branches, and it ensures the value is set just once
        final RexDigestIncludeType includeType;
        if ( type.getPolyType() == PolyType.BOOLEAN
                || type.getPolyType() == PolyType.INTEGER
                || type.getPolyType() == PolyType.SYMBOL ) {
            // We don't want false:BOOLEAN NOT NULL, so we don't print type information for non-nullable BOOLEAN and INTEGER
            includeType = RexDigestIncludeType.NO_TYPE;
        } else if ( type.getPolyType() == PolyType.CHAR && value instanceof NlsString ) {
            NlsString nlsString = (NlsString) value;

            // Ignore type information for 'Bar':CHAR(3)
            if ( ((nlsString.getCharset() != null
                    && type.getCharset().equals( nlsString.getCharset() ))
                    || (nlsString.getCharset() == null
                    && Collation.IMPLICIT.getCharset().equals( type.getCharset() )))
                    && nlsString.getCollation().equals( type.getCollation() )
                    && ((NlsString) value).getValue().length() == type.getPrecision() ) {
                includeType = RexDigestIncludeType.NO_TYPE;
            } else {
                includeType = RexDigestIncludeType.ALWAYS;
            }
        } else if ( type.getPrecision() == 0 && (
                type.getPolyType() == PolyType.TIME
                        || type.getPolyType() == PolyType.TIMESTAMP
                        || type.getPolyType() == PolyType.DATE) ) {
            // Ignore type information for '12:23:20':TIME(0)
            // Note that '12:23:20':TIME WITH LOCAL TIME ZONE
            includeType = RexDigestIncludeType.NO_TYPE;
        } else {
            includeType = RexDigestIncludeType.ALWAYS;
        }
        return includeType;
    }


    /**
     * Returns whether a value is valid as a constant value, using the same criteria as {@link #valueMatchesType}.
     */
    public static boolean validConstant( Object o, Litmus litmus ) {
        if ( o == null
                || o instanceof BigDecimal
                || o instanceof NlsString
                || o instanceof ByteString ) {
            return litmus.succeed();
        } else if ( o instanceof List ) {
            @SuppressWarnings("unchecked") List<Object> list = (List<Object>) o;
            for ( Object o1 : list ) {
                if ( !validConstant( o1, litmus ) ) {
                    return litmus.fail( "not a constant: {}", o1 );
                }
            }
            return litmus.succeed();
        } else if ( o instanceof Map ) {
            @SuppressWarnings("unchecked") final Map<Object, Object> map = (Map<Object, Object>) o;
            for ( Map.Entry<Object, Object> entry : map.entrySet() ) {
                if ( !validConstant( entry.getKey(), litmus ) ) {
                    return litmus.fail( "not a constant: {}", entry.getKey() );
                }
                if ( !validConstant( entry.getValue(), litmus ) ) {
                    return litmus.fail( "not a constant: {}", entry.getValue() );
                }
            }
            return litmus.succeed();
        } else {
            return litmus.fail( "not a constant: {}", o );
        }
    }


    /**
     * Returns a list of the time units covered by an interval type such as HOUR TO SECOND. Adds MILLISECOND if the end is SECOND, to deal with fractional seconds.
     */
    private static List<TimeUnit> getTimeUnits( PolyType typeName ) {
        final TimeUnit start = typeName.getStartUnit();
        final TimeUnit end = typeName.getEndUnit();
        final ImmutableList<TimeUnit> list = TIME_UNITS.subList( start.ordinal(), end.ordinal() + 1 );
        if ( end == TimeUnit.SECOND ) {
            return CompositeList.of( list, ImmutableList.of( TimeUnit.MILLISECOND ) );
        }
        return list;
    }


    private String intervalString( BigDecimal v ) {
        final List<TimeUnit> timeUnits = getTimeUnits( type.getPolyType() );
        final StringBuilder b = new StringBuilder();
        for ( TimeUnit timeUnit : timeUnits ) {
            final BigDecimal[] result = v.divideAndRemainder( timeUnit.multiplier );
            if ( b.length() > 0 ) {
                b.append( timeUnit.separator );
            }
            final int width = b.length() == 0 ? -1 : width( timeUnit ); // don't pad 1st
            pad( b, result[0].toString(), width );
            v = result[1];
        }
        if ( Util.last( timeUnits ) == TimeUnit.MILLISECOND ) {
            while ( b.toString().matches( ".*\\.[0-9]*0" ) ) {
                if ( b.toString().endsWith( ".0" ) ) {
                    b.setLength( b.length() - 2 ); // remove ".0"
                } else {
                    b.setLength( b.length() - 1 ); // remove "0"
                }
            }
        }
        return b.toString();
    }


    private static void pad( StringBuilder b, String s, int width ) {
        if ( width >= 0 ) {
            for ( int i = s.length(); i < width; i++ ) {
                b.append( '0' );
            }
        }
        b.append( s );
    }


    private static int width( TimeUnit timeUnit ) {
        switch ( timeUnit ) {
            case MILLISECOND:
                return 3;
            case HOUR:
            case MINUTE:
            case SECOND:
                return 2;
            default:
                return -1;
        }
    }


    /**
     * Prints the value this literal as a Java string constant.
     */
    public void printAsJava( PrintWriter pw ) {
        printAsJava( value, pw, typeName, true, RexDigestIncludeType.NO_TYPE );
    }


    /**
     * Prints a value as a Java string. The value must be consistent with the type, as per {@link #valueMatchesType}.
     *
     * Typical return values:
     *
     * <ul>
     * <li>true</li>
     * <li>null</li>
     * <li>"Hello, world!"</li>
     * <li>1.25</li>
     * <li>1234ABCD</li>
     * </ul>
     *
     * @param value Value
     * @param pw Writer to write to
     * @param typeName Type family
     * @param includeType if representation should include data type
     */
    private static void printAsJava( Comparable<?> value, PrintWriter pw, PolyType typeName, boolean java, RexDigestIncludeType includeType ) {
        switch ( typeName ) {
            case CHAR:
                NlsString nlsString = (NlsString) value;
                if ( java ) {
                    Util.printJavaString( pw, nlsString.getValue(), true );
                } else {
                    boolean includeCharset = (nlsString.getCharsetName() != null) && !nlsString.getCharsetName().equals( SaffronProperties.INSTANCE.defaultCharset().get() );
                    pw.print( nlsString.asSql( includeCharset, false ) );
                }
                break;
            case BOOLEAN:
                assert value instanceof Boolean;
                pw.print( ((Boolean) value).booleanValue() );
                break;
            case DECIMAL:
                assert value instanceof BigDecimal;
                pw.print( value );
                break;
            case DOUBLE:
                assert value instanceof BigDecimal;
                pw.print( Util.toScientificNotation( (BigDecimal) value ) );
                break;
            case BIGINT:
                assert value instanceof BigDecimal;
                pw.print( ((BigDecimal) value).longValue() );
                pw.print( 'L' );
                break;
            case BINARY:
                assert value instanceof ByteString;
                pw.print( "X'" );
                pw.print( ((ByteString) value).toString( 16 ) );
                pw.print( "'" );
                break;
            case NULL:
                assert value == null;
                pw.print( "null" );
                break;
            case SYMBOL:
                assert value instanceof Enum;
                pw.print( "FLAG(" );
                pw.print( value );
                pw.print( ")" );
                break;
            case DATE:
                assert value instanceof DateString;
                pw.print( value );
                break;
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
                assert value instanceof TimeString;
                pw.print( value );
                break;
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                assert value instanceof TimestampString;
                pw.print( value );
                break;
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                if ( value instanceof BigDecimal ) {
                    pw.print( value.toString() );
                } else {
                    assert value == null;
                    pw.print( "null" );
                }
                break;
            case MULTISET:
            case ARRAY:
            case ROW:
                @SuppressWarnings("unchecked") final List<RexLiteral> list = (List<RexLiteral>) value;
                pw.print(
                        new AbstractList<String>() {
                            @Override
                            public String get( int index ) {
                                return list.get( index ).computeDigest( includeType );
                            }


                            @Override
                            public int size() {
                                return list.size();
                            }
                        } );
                break;
            case MAP:
                @SuppressWarnings("unchecked") final Map<RexLiteral, RexLiteral> map = (Map<RexLiteral, RexLiteral>) value;
                pw.print(
                        new AbstractMap<String, String>() {
                            @Override
                            public Set<Entry<String, String>> entrySet() {
                                return map
                                        .entrySet()
                                        .stream()
                                        .map( e -> new SimpleImmutableEntry<>( e.getKey().computeDigest( includeType ), e.getValue().computeDigest( includeType ) ) )
                                        .collect( Collectors.toSet() );
                            }
                        }
                );
                break;
            case NODE:
                assert value instanceof PolyNode;
                pw.print( value );
                break;
            case EDGE:
                assert value instanceof PolyEdge;
                pw.print( value );
                break;
            case GRAPH:
                assert value instanceof PolyGraph;
                pw.print( value );
                break;
            case PATH:
                assert value instanceof PolyPath;
                pw.print( value );
                break;
            default:
                assert valueMatchesType( value, typeName, true );
                throw Util.needToImplement( typeName );
        }
    }


    private static String getCalendarFormat( PolyType typeName ) {
        switch ( typeName ) {
            case DATE:
                return DateTimeUtils.DATE_FORMAT_STRING;
            case TIME:
                return DateTimeUtils.TIME_FORMAT_STRING;
            case TIMESTAMP:
                return DateTimeUtils.TIMESTAMP_FORMAT_STRING;
            default:
                throw new AssertionError( "getCalendarFormat: unknown type" );
        }
    }


    public PolyType getTypeName() {
        return typeName;
    }


    @Override
    public AlgDataType getType() {
        return type;
    }


    @Override
    public Kind getKind() {
        return Kind.LITERAL;
    }


    /**
     * Returns whether this literal's value is null.
     */
    public boolean isNull() {
        return value == null;
    }


    /**
     * Returns the value of this literal.
     *
     * For backwards compatibility, returns DATE. TIME and TIMESTAMP as a {@link Calendar} value in UTC time zone.
     */
    public Comparable getValue() {
        assert valueMatchesType( value, typeName, true ) : value;
        if ( value == null ) {
            return null;
        }
        switch ( typeName ) {
            case TIME:
            case DATE:
            case TIMESTAMP:
                return getValueAs( Calendar.class );
            case MAP:
                return getValueAsPolyMap();
            default:
                return value;
        }
    }


    private PolyMap<Comparable<?>, Comparable<?>> getValueAsPolyMap() {
        return new PolyMap<Comparable<?>, Comparable<?>>( ((Map<RexLiteral, RexLiteral>) value).entrySet().stream()
                .collect( Collectors.toMap( e -> e.getKey().getValueForQueryParameterizer(), e -> e.getValue().getValueForQueryParameterizer() ) ) );
    }


    /**
     * Returns the value of this literal as required by the query parameterizer.
     */
    public Comparable getValueForQueryParameterizer() {
        assert valueMatchesType( value, typeName, true ) : value;
        if ( value == null ) {
            return null;
        }
        switch ( type.getPolyType() ) {
            case TIME:
                return getValueAs( TimeString.class );
            case DATE:
                return getValueAs( DateString.class );
            case TIMESTAMP:
                return getValueAs( TimestampString.class );
            case CHAR:
            case VARCHAR:
                return getValueAs( String.class );
            case BOOLEAN:
                return getValueAs( Boolean.class );
            case TINYINT:
                return getValueAs( Byte.class );
            case SMALLINT:
                return getValueAs( Short.class );
            case INTEGER:
                return getValueAs( Integer.class );
            case BIGINT:
                return getValueAs( Long.class );
            case DECIMAL:
                return getValueAs( BigDecimal.class );
            case FLOAT:
            case REAL:
                return getValueAs( Float.class );
            case DOUBLE:
                return getValueAs( Double.class );
            case ARRAY:
                return ((List<RexLiteral>) value).stream().map( RexLiteral::getValueForQueryParameterizer ).collect( Collectors.toCollection( PolyList::new ) );
            case MAP:
                return getValueAsPolyMap();
            /*case BINARY:
            case VARBINARY:
                break;
            case ARRAY:
                break;*/

            default:
                return value;
        }
    }


    /**
     * Returns the value of this literal, in the form that the calculator program builder wants it.
     */
    public Object getValue2() {
        if ( value == null ) {
            return null;
        }
        switch ( typeName ) {
            case CHAR:
                return getValueAs( String.class );
            case DECIMAL:
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return getValueAs( Long.class );
            case DATE:
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
                return getValueAs( Integer.class );
            default:
                return value;
        }
    }


    /**
     * Returns the value of this literal, in the form that the rex-to-lix translator wants it.
     */
    public Object getValue3() {
        if ( value == null ) {
            return null;
        }
        switch ( typeName ) {
            case DECIMAL:
                assert value instanceof BigDecimal;
                return value;
            default:
                return getValue2();
        }
    }


    /**
     * Returns the value of this literal, in the form that {@link RexInterpreter} wants it.
     */
    public Comparable getValue4() {
        if ( value == null ) {
            return null;
        }
        switch ( typeName ) {
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return getValueAs( Long.class );
            case DATE:
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
                return getValueAs( Integer.class );
            default:
                return value;
        }
    }


    /**
     * Returns the value of this literal as an instance of the specified class.
     *
     * The following SQL types allow more than one form:
     *
     * <ul>
     * <li>CHAR as {@link NlsString} or {@link String}</li>
     * <li>TIME as {@link TimeString}, {@link Integer} (milliseconds since midnight), {@link Calendar} (in UTC)</li>
     * <li>DATE as {@link DateString}, {@link Integer} (days since 1970-01-01), {@link Calendar}</li>
     * <li>TIMESTAMP as {@link TimestampString}, {@link Long} (milliseconds since 1970-01-01 00:00:00), {@link Calendar}</li>
     * <li>DECIMAL as {@link BigDecimal} or {@link Long}</li>
     * </ul>
     *
     * Called with {@code clazz} = {@link Comparable}, returns the value in its native form.
     *
     * @param clazz Desired return type
     * @param <T> Return type
     * @return Value of this literal in the desired type
     */
    public <T> T getValueAs( Class<T> clazz ) {
        if ( value == null || clazz.isInstance( value ) ) {
            return clazz.cast( value );
        }
        switch ( typeName ) {
            case BINARY:
                if ( clazz == byte[].class ) {
                    return clazz.cast( ((ByteString) value).getBytes() );
                }
                break;
            case CHAR:
                if ( clazz == String.class ) {
                    return clazz.cast( ((NlsString) value).getValue() );
                } else if ( clazz == Character.class ) {
                    return clazz.cast( ((NlsString) value).getValue().charAt( 0 ) );
                }
                break;
            case VARCHAR:
                if ( clazz == String.class ) {
                    return clazz.cast( ((NlsString) value).getValue() );
                }
                break;
            case DECIMAL:
                if ( clazz == Long.class ) {
                    return clazz.cast( ((BigDecimal) value).unscaledValue().longValue() );
                }
                // fall through
            case BIGINT:
            case INTEGER:
            case SMALLINT:
            case TINYINT:
            case DOUBLE:
            case REAL:
            case FLOAT:
                if ( clazz == Long.class ) {
                    return clazz.cast( ((BigDecimal) value).longValue() );
                } else if ( clazz == Integer.class ) {
                    return clazz.cast( ((BigDecimal) value).intValue() );
                } else if ( clazz == Short.class ) {
                    return clazz.cast( ((BigDecimal) value).shortValue() );
                } else if ( clazz == Byte.class ) {
                    return clazz.cast( ((BigDecimal) value).byteValue() );
                } else if ( clazz == Double.class ) {
                    return clazz.cast( ((BigDecimal) value).doubleValue() );
                } else if ( clazz == Float.class ) {
                    return clazz.cast( ((BigDecimal) value).floatValue() );
                }
                break;
            case DATE:
                if ( clazz == Integer.class ) {
                    return clazz.cast( ((DateString) value).getDaysSinceEpoch() );
                } else if ( clazz == Calendar.class ) {
                    return clazz.cast( ((DateString) value).toCalendar() );
                }
                break;
            case TIME:
                if ( clazz == Integer.class ) {
                    return clazz.cast( ((TimeString) value).getMillisOfDay() );
                } else if ( clazz == Calendar.class ) {
                    // Note: Nanos are ignored
                    return clazz.cast( ((TimeString) value).toCalendar() );
                }
                break;
            case TIME_WITH_LOCAL_TIME_ZONE:
                if ( clazz == Integer.class ) {
                    // Milliseconds since 1970-01-01 00:00:00
                    return clazz.cast( ((TimeString) value).getMillisOfDay() );
                }
                break;
            case TIMESTAMP:
                if ( clazz == Long.class ) {
                    // Milliseconds since 1970-01-01 00:00:00
                    return clazz.cast( ((TimestampString) value).getMillisSinceEpoch() );
                } else if ( clazz == Calendar.class ) {
                    // Note: Nanos are ignored
                    return clazz.cast( ((TimestampString) value).toCalendar() );
                }
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if ( clazz == Long.class ) {
                    // Milliseconds since 1970-01-01 00:00:00
                    return clazz.cast( ((TimestampString) value).getMillisSinceEpoch() );
                } else if ( clazz == Calendar.class ) {
                    // Note: Nanos are ignored
                    return clazz.cast( ((TimestampString) value).toCalendar() );
                }
                break;
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                if ( clazz == Integer.class ) {
                    return clazz.cast( ((BigDecimal) value).intValue() );
                } else if ( clazz == Long.class ) {
                    return clazz.cast( ((BigDecimal) value).longValue() );
                } else if ( clazz == String.class ) {
                    return clazz.cast( intervalString( getValueAs( BigDecimal.class ).abs() ) );
                } else if ( clazz == Boolean.class ) {
                    // return whether negative
                    return clazz.cast( getValueAs( BigDecimal.class ).signum() < 0 );
                }
                break;
            case MAP:
                if ( clazz == Map.class ) {
                    return clazz.cast( value );
                }
                break;
            case ARRAY:
                if ( clazz == List.class ) {
                    return clazz.cast( value );
                } else if ( clazz == String.class ) {
                    return clazz.cast( gson.toJson( ((List<RexLiteral>) value).stream().map( RexLiteral::getValueForQueryParameterizer ).collect( Collectors.toList() ) ) );
                }
                break;
            case NODE:
                if ( clazz == PolyNode.class ) {
                    return clazz.cast( value );
                }
                break;
            case EDGE:
                if ( clazz == PolyEdge.class ) {
                    return clazz.cast( value );
                }
                break;
            case PATH:
                if ( clazz == PolyPath.class ) {
                    return clazz.cast( value );
                }
                break;
        }
        throw new AssertionError( "cannot convert " + typeName + " literal to " + clazz );
    }


    public String getValueForFileAdapter() {
        if ( value == null ) {
            return null;
        }
        switch ( typeName ) {
            case VARCHAR:
            case CHAR:
                return ((NlsString) value).getValue();
            case BOOLEAN:
                return Boolean.toString( (Boolean) value );
            case DATE:
            case TIME:
                int i = getValueAs( Integer.class );
                return String.valueOf( i );
            case TIMESTAMP:
                long l = getValueAs( Long.class );
                return String.valueOf( l );
            case BINARY:
                return new String( getValueAs( byte[].class ), StandardCharsets.UTF_8 );
            default:
                return value.toString();
        }
    }


    /**
     * see {@code org.polypheny.db.adapter.file.Condition}
     */
    public Comparable getValueForFileCondition() {
        switch ( typeName ) {
            case TIME:
            case DATE:
                return getValueAs( Integer.class );
            case TIMESTAMP:
                return getValueAs( Long.class );
            default:
                return getValueForQueryParameterizer();
        }
    }


    public static boolean booleanValue( RexNode node ) {
        return (Boolean) ((RexLiteral) node).value;
    }


    @Override
    public boolean isAlwaysTrue() {
        if ( typeName != PolyType.BOOLEAN ) {
            return false;
        }
        return booleanValue( this );
    }


    @Override
    public boolean isAlwaysFalse() {
        if ( typeName != PolyType.BOOLEAN ) {
            return false;
        }
        return !booleanValue( this );
    }


    public boolean equals( Object obj ) {
        return (obj instanceof RexLiteral)
                && equals( ((RexLiteral) obj).value, value )
                && equals( ((RexLiteral) obj).type, type );
    }


    public int hashCode() {
        return Objects.hash( value, type );
    }


    public static Comparable value( RexNode node ) {
        return findValue( node );
    }


    public static int intValue( RexNode node ) {
        final Comparable value = findValue( node );
        return ((Number) value).intValue();
    }


    public static String stringValue( RexNode node ) {
        final Comparable value = findValue( node );
        return (value == null) ? null : ((NlsString) value).getValue();
    }


    private static Comparable findValue( RexNode node ) {
        if ( node instanceof RexLiteral ) {
            return ((RexLiteral) node).value;
        }
        if ( node instanceof RexCall ) {
            final RexCall call = (RexCall) node;
            final Operator operator = call.getOperator();
            if ( operator.getOperatorName() == OperatorName.CAST ) {
                return findValue( call.getOperands().get( 0 ) );
            }
            if ( operator.getOperatorName() == OperatorName.UNARY_MINUS ) {
                final BigDecimal value = (BigDecimal) findValue( call.getOperands().get( 0 ) );
                return value.negate();
            }
        }
        throw new AssertionError( "not a literal: " + node );
    }


    public static boolean isNullLiteral( RexNode node ) {
        return (node instanceof RexLiteral) && (((RexLiteral) node).value == null);
    }


    private static boolean equals( Object o1, Object o2 ) {
        return Objects.equals( o1, o2 );
    }


    @Override
    public <R> R accept( RexVisitor<R> visitor ) {
        return visitor.visitLiteral( this );
    }


    @Override
    public <R, P> R accept( RexBiVisitor<R, P> visitor, P arg ) {
        return visitor.visitLiteral( this, arg );
    }


    @Override
    public int compareTo( RexLiteral o ) {
        if ( !this.value.getClass().equals( o.value.getClass() ) ) {
            return -1;
        }

        int comp = this.value.compareTo( o.value );

        if ( comp != 0 ) {
            return -1;
        }

        return this.digest.equals( o.digest )
                ? 0 : this.digest.length() > o.digest.length()
                ? 1 : -1;
    }


    public List<RexLiteral> getRexList() {
        assert value instanceof PolyList;
        return (List<RexLiteral>) value;
    }

}

