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

package org.polypheny.db.rex;


import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Value;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.util.Collation;
import org.polypheny.db.util.NlsString;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.TimestampString;
import org.polypheny.db.util.Unsafe;
import org.polypheny.db.util.Util;
import org.polypheny.db.util.temporal.TimeUnit;


/**
 * Constant value in a row-expression.
 * <p>
 * There are several methods for creating literals in {@link RexBuilder}: {@link RexBuilder#makeLiteral(boolean)} and so forth.
 * <p>
 * How is the value stored? In that respect, the class is somewhat of a black box. There is a {@link #getValue} method which returns the value as an object, but the type of that value is implementation detail,
 * and it is best that your code does not depend upon that knowledge. It is better to use task-oriented methods such as {@link #getValue} and {@link #toJavaString}.
 * <p>
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
 * <td>{@link PolyType#INTERVAL},
 * <td>Interval, for example <code>INTERVAL '4:3:2' HOUR TO SECOND</code></td>
 * <td>{@link BigDecimal}; also {@link Long} (milliseconds)</td>
 * </tr>
 * <tr>
 * <td> {@link PolyType#INTERVAL}</td>
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
@Getter
@Value
public class RexLiteral extends RexNode implements Comparable<RexLiteral> {

    /**
     * The value of this literal. Must be consistent with its type, as per {@link #valueMatchesType}. For example, you can't store an {@link Integer} value here just because you feel like it -- all numbers are
     * represented by a {@link BigDecimal}. But since this field is private, it doesn't really matter how the values are stored.
     */
    public PolyValue value;

    /**
     * The real type of this literal, as reported by {@link #getType}.
     */
    public AlgDataType type;

    // TODO jvs: Use SqlTypeFamily instead; it exists for exactly this purpose (to avoid the confusion which results from overloading PolyType).
    /**
     * An indication of the broad type of this literal -- even if its type isn't a SQL type. Sometimes this will be different than the SQL type; for example, all exact numbers, including integers have typeName
     * {@link PolyType#DECIMAL}. See {@link #valueMatchesType} for the definitive story.
     */
    public PolyType polyType;

    private static final ImmutableList<TimeUnit> TIME_UNITS = ImmutableList.copyOf( TimeUnit.values() );


    /**
     * Creates a <code>RexLiteral</code>.
     */
    public RexLiteral( PolyValue value, AlgDataType type, PolyType polyType ) {
        this.value = value;
        this.type = Objects.requireNonNull( type );
        this.polyType = Objects.requireNonNull( polyType );
        if ( !valueMatchesType( value, polyType, true ) ) {
            System.err.println( value );
            System.err.println( value.getClass().getCanonicalName() );
            System.err.println( type );
            System.err.println( polyType );
            throw new IllegalArgumentException();
        }
//        Preconditions.checkArgument( valueMatchesType( value, typeName, true ) );
        Preconditions.checkArgument( (value != null) || type.isNullable() );
        Preconditions.checkArgument( polyType != PolyType.ANY );
        this.digest = computeDigest( RexDigestIncludeType.OPTIONAL );
    }


    public RexLiteral( PolyValue value, AlgDataType type, PolyType polyType, boolean raw ) {
        this.value = value;
        this.type = Objects.requireNonNull( type );
        this.polyType = Objects.requireNonNull( polyType );
        this.digest = computeDigest( RexDigestIncludeType.OPTIONAL );
    }


    /**
     * Returns a string which concisely describes the definition of this rex literal. Two literals are equivalent if and only if their digests are the same.
     * <p>
     * The digest does not contain the expression's identity, but does include the identity of children.
     * <p>
     * Technically speaking 1:INT differs from 1:FLOAT, so we need data type in the literal's digest, however we want to avoid extra verbosity of the {@link AlgNode#getDigest()} for readability purposes, so we omit type info in certain cases.
     * For instance, 1:INT becomes 1 (INT is implied by default), however 1:BIGINT always holds the type
     * <p>
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
    public String computeDigest( RexDigestIncludeType includeType ) {
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

        return toJavaString( value, polyType, type, includeType );
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


    public static Pair<PolyValue, PolyType> convertType( PolyValue value, AlgDataType typeName ) {
        PolyValue converted = PolyValue.convert( value, typeName.getPolyType() );
        return new Pair<>( converted, typeName.getPolyType() );
    }


    /**
     * @return whether value is appropriate for its type (we have rules about these things)
     */
    public static boolean valueMatchesType( PolyValue value, PolyType typeName, boolean strict ) {
        if ( value == null || value.isNull() ) {
            return true;
        }
        return switch ( typeName ) {
            case BOOLEAN ->
                // Unlike SqlLiteral, we do not allow boolean null.
                    value.isBoolean();
            case NULL -> false; // value should have been null
            // not allowed -- use Decimal
            case INTEGER, TINYINT, SMALLINT, DECIMAL, DOUBLE, FLOAT, REAL, BIGINT -> value.isNumber();
            case DATE -> value.isDate();
            case TIME -> value.isTime();
            case TIMESTAMP -> value.isTimestamp();
            case INTERVAL ->
                // The value of a DAY-TIME interval (whatever the start and end units, even say HOUR TO MINUTE) is in milliseconds (perhaps fractional milliseconds). The value of a YEAR-MONTH interval is in months.
                    value.isInterval();
            case VARBINARY -> // not allowed -- use Binary
                    value.isBinary();
            case BINARY -> value.isBinary();
            case VARCHAR, CHAR ->
                // A SqlLiteral's charset and collation are optional; not so a RexLiteral.
                    value.isString();
            case SYMBOL -> value.isSymbol();
            case ROW, MULTISET, ARRAY -> value.isList();
            case ANY ->
                // Literal of type ANY is not legal. "CAST(2 AS ANY)" remains an integer literal surrounded by a cast function.
                    false;
            case GRAPH -> value.isGraph();
            case NODE -> value.isNode();
            case EDGE -> value.isEdge();
            case PATH -> value.isPath();
            case MAP -> value.isMap();
            case DOCUMENT -> true;
            default -> throw Util.unexpected( typeName );
        };
    }


    private static String toJavaString( PolyValue value, PolyType typeName, AlgDataType type, RexDigestIncludeType includeType ) {
        assert includeType != RexDigestIncludeType.OPTIONAL : "toJavaString must not be called with includeType=OPTIONAL";
        String fullTypeString = type.getFullTypeString();
        if ( value == null ) {
            return includeType == RexDigestIncludeType.NO_TYPE ? "null" : "null:" + fullTypeString;
        }
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter( sw );
        printAsJava( value, pw, typeName, false );
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
     * <p>
     * Implementation assumption: this method should be fast. In fact might call {@link NlsString#getValue()} which could decode the string, however we rely on the cache there.
     *
     * @param value value of the literal
     * @param type type of the literal
     * @return NO_TYPE when type can be omitted, ALWAYS otherwise
     * @see RexLiteral#computeDigest(RexDigestIncludeType)
     */
    private static RexDigestIncludeType shouldIncludeType( PolyValue value, AlgDataType type ) {
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
        } else if ( PolyType.STRING_TYPES.contains( type.getPolyType() ) && value.isString() ) {
            PolyString string = value.asString();

            // Ignore type information for 'Bar':CHAR(3)
            if ( ((string.getCharset() != null
                    && type.getCharset().name().equals( string.getCharset().name() ))
                    || (string.getCharset() == null
                    && Collation.IMPLICIT.getCharset().name().equals( type.getCharset().name() )))
                    && string.value.length() == type.getPrecision() ) {
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


    public static void pad( StringBuilder b, String s, int width ) {
        if ( width >= 0 ) {
            b.append( "0".repeat( Math.max( 0, width - s.length() ) ) );
        }
        b.append( s );
    }


    public static int width( TimeUnit timeUnit ) {
        return switch ( timeUnit ) {
            case MILLISECOND -> 3;
            case HOUR, MINUTE, SECOND -> 2;
            default -> -1;
        };
    }


    /**
     * Prints a value as a Java string. The value must be consistent with the type, as per {@link #valueMatchesType}.
     * <p>
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
     */
    private static void printAsJava( PolyValue value, PrintWriter pw, PolyType typeName, boolean java ) {
        switch ( typeName ) {
            case VARCHAR:
            case CHAR:
                PolyString string = value.asString();
                if ( java ) {
                    Util.printJavaString( pw, value.asString().getValue(), true );
                } else {
                    boolean includeCharset = (string.charset != null) && !string.charset.equals( PolyValue.CHARSET );
                    pw.print( string.toTypedString( includeCharset ) );
                }
                break;
            case BOOLEAN:
                assert value.isBoolean();
                pw.print( value.asBoolean().value );
                break;
            case DECIMAL:
                assert value.isBigDecimal();
                pw.print( value.asBigDecimal().value );
                break;
            case DOUBLE:
                assert value.isNumber();
                pw.print( Util.toScientificNotation( value.asNumber().BigDecimalValue() ) );
                break;
            case BIGINT:
                assert value.isBigDecimal();
                pw.print( value.asNumber().bigDecimalValue() );
                pw.print( 'L' );
                break;
            case INTEGER, SMALLINT, TINYINT:
                assert value.isNumber();
                pw.print( value.asNumber().intValue() );
                break;
            case REAL:
                assert value.isNumber();
                pw.print( value.asNumber().floatValue() );
                pw.print( 'R' );
                break;
            case BINARY:
            case VARBINARY:
                assert value.isBinary();
                pw.print( "X'" );
                pw.print( value.asBinary().value );
                pw.print( "'" );
                break;
            case NULL:
                assert value == null;
                pw.print( "null" );
                break;
            case SYMBOL:
                assert value.isSymbol();
                pw.print( "FLAG(" );
                pw.print( value );
                pw.print( ")" );
                break;
            case DATE:
                assert value.isDate();
                pw.print( value.toJson() );
                break;
            case TIME:
                assert value.isTime();
                pw.print( value.toJson() );
                break;
            case TIMESTAMP:
                assert value.isTimestamp();
                pw.print( value.asTimestamp() );
                break;
            case INTERVAL:
                assert value.isInterval();
                pw.print( value.asInterval().getMonths() + "-" + value.asInterval().getMillis() );
                break;
            case ARRAY:
                pw.print( value.asList().stream().map( e -> e == null ? "" : e.toString() ).toList() );
                break;
            case MULTISET:
            case ROW:
                final List<PolyValue> list = value.asList();
                pw.print( list.stream().map( PolyValue::toString ).collect( Collectors.toList() ) );
                break;
            case MAP:
                final Map<PolyValue, PolyValue> map = value.asMap();
                pw.print( map.entrySet().stream().map( Object::toString ).collect( Collectors.toList() ) );
                break;
            case NODE:
                assert value.isNode();
                pw.print( value );
                break;
            case EDGE:
                assert value.isEdge();
                pw.print( value );
                break;
            case GRAPH:
                assert value.isGraph();
                pw.print( value );
                break;
            case PATH:
                assert value.isPath();
                pw.print( value );
                break;
            case DOCUMENT:
                // assert value.isDocument(); documents can be any PolyValue
                pw.println( value );
                break;
            default:
                assert valueMatchesType( value, typeName, true );
                throw Util.needToImplement( typeName );
        }
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


    @Override
    public String toString() {
        return super.toString();
    }


    public static boolean booleanValue( RexNode node ) {
        return ((RexLiteral) node).value.isBoolean() ? ((RexLiteral) node).value.asBoolean().value : false;
    }


    @Override
    public boolean isAlwaysTrue() {
        if ( polyType != PolyType.BOOLEAN ) {
            return false;
        }
        return booleanValue( this );
    }


    @Override
    public boolean isAlwaysFalse() {
        if ( polyType != PolyType.BOOLEAN ) {
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


    public static PolyValue value( RexNode node ) {
        return findValue( node );
    }


    public static int intValue( RexNode node ) {
        final PolyValue value = findValue( node );
        return value.asNumber().intValue();
    }


    public static PolyString stringValue( RexNode node ) {
        final PolyValue value = findValue( node );
        return (value == null) ? null : value.asString();
    }


    private static PolyValue findValue( RexNode node ) {
        if ( node instanceof RexLiteral ) {
            return ((RexLiteral) node).value;
        }
        if ( node instanceof RexCall call ) {
            final Operator operator = call.getOperator();
            if ( operator.getOperatorName() == OperatorName.CAST ) {
                return findValue( call.getOperands().get( 0 ) );
            }
            if ( operator.getOperatorName() == OperatorName.UNARY_MINUS ) {
                final PolyNumber value = findValue( call.getOperands().get( 0 ) ).asNumber();
                return PolyBigDecimal.of( value.asBigDecimal().bigDecimalValue().negate() );
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

}

