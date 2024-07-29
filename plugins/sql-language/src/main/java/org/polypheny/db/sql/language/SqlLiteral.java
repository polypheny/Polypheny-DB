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


import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Calendar;
import java.util.List;
import java.util.Objects;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.Monotonicity;
import org.polypheny.db.algebra.json.JsonEmptyOrError;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Literal;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.NodeVisitor;
import org.polypheny.db.nodes.TimeUnitRange;
import org.polypheny.db.sql.language.fun.SqlLiteralChainOperator;
import org.polypheny.db.sql.language.parser.SqlParserUtil;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.sql.language.validate.SqlValidatorScope;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.type.entity.PolyBinary;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyInterval;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolySymbol;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.type.entity.temporal.PolyTime;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;
import org.polypheny.db.util.BitString;
import org.polypheny.db.util.Collation;
import org.polypheny.db.util.CoreUtil;
import org.polypheny.db.util.Litmus;
import org.polypheny.db.util.NlsString;
import org.polypheny.db.util.Static;
import org.polypheny.db.util.Util;


/**
 * A <code>SqlLiteral</code> is a constant. It is, appropriately, immutable.
 * <p>
 * How is the value stored? In that respect, the class is somewhat of a black box. There is a {@link #getValue} method which returns the value as an object, but the type of that value is implementation detail, and it is best
 * that your code does not depend upon that knowledge. It is better to use task-oriented methods such as {@link #toSqlString(SqlDialect)} and {@link #toValue}.
 * <p>
 * If you really need to access the value directly, you should switch on the value of the {@link #typeName} field, rather than making assumptions about the runtime type of the {@link #value}.
 * <p>
 * The allowable types and combinations are:
 *
 * <table>
 * <caption>Allowable types for SqlLiteral</caption>
 * <tr>
 * <th>TypeName</th>
 * <th>Meaing</th>
 * <th>Value type</th>
 * </tr>
 * <tr>
 * <td>{@link PolyType#NULL}</td>
 * <td>The null value. It has its own special type.</td>
 * <td>null</td>
 * </tr>
 * <tr>
 * <td>{@link PolyType#BOOLEAN}</td>
 * <td>Boolean, namely <code>TRUE</code>, <code>FALSE</code> or <code>UNKNOWN</code>.</td>
 * <td>{@link Boolean}, or null represents the UNKNOWN value</td>
 * </tr>
 * <tr>
 * <td>{@link PolyType#DECIMAL}</td>
 * <td>Exact number, for example <code>0</code>, <code>-.5</code>, <code>12345</code>.</td>
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
 * <td>{@link Calendar}</td>
 * </tr>
 * <tr>
 * <td>{@link PolyType#TIME}</td>
 * <td>Time, for example <code>TIME '18:37:42.567'</code></td>
 * <td>{@link Calendar}</td>
 * </tr>
 * <tr>
 * <td>{@link PolyType#TIMESTAMP}</td>
 * <td>Timestamp, for example <code>TIMESTAMP '1969-04-29 18:37:42.567'</code></td>
 * <td>{@link Calendar}</td>
 * </tr>
 * <tr>
 * <td>{@link PolyType#CHAR}</td>
 * <td>Character constant, for example <code>'Hello, world!'</code>, <code>''</code>, <code>_N'Bonjour'</code>, <code>_ISO-8859-1'It''s superman!' COLLATE SHIFT_JIS$ja_JP$2</code>. These are always CHAR, never VARCHAR.</td>
 * <td>{@link NlsString}</td>
 * </tr>
 * <tr>
 * <td>{@link PolyType#BINARY}</td>
 * <td>Binary constant, for example <code>X'ABC'</code>, <code>X'7F'</code>. Note that strings with an odd number of hexits will later become values of the BIT datatype, because they have an incomplete number of bytes. But here, they are all binary constants, because that's how they were written. These constants are always BINARY, never VARBINARY.</td>
 * <td>{@link BitString}</td>
 * </tr>
 * <tr>
 * <td>{@link PolyType#SYMBOL}</td>
 * <td>A symbol is a special type used to make parsing easier; it is not part of the SQL standard, and is not exposed to end-users. It is used to hold a symbol, such as the LEADING flag in a call to the function <code>TRIM([LEADING|TRAILING|BOTH] chars FROM string)</code>.</td>
 * <td>An {@link Enum}</td>
 * </tr>
 * <tr>
 * <td>Interval, for example <code>INTERVAL '1:34' HOUR</code>.</td>
 * <td>{@link SqlIntervalLiteral.IntervalValue}.</td>
 * </tr>
 * </table>
 */
@Slf4j
@Getter
public class SqlLiteral extends SqlNode implements Literal {

    /**
     * The type with which this literal was declared. This type is very approximate: the literal may have a different type once validated. For example, all numeric literals have a type name of
     * {@link PolyType#DECIMAL}, but on validation may become {@link PolyType#INTEGER}.
     */
    private final PolyType typeName;

    /**
     * The value of this literal. The type of the value must be appropriate for the typeName, as defined by the {@link #valueMatchesType} method.
     */
    public final PolyValue value;


    /**
     * Creates a <code>SqlLiteral</code>.
     */
    protected SqlLiteral( PolyValue value, PolyType typeName, ParserPos pos ) {
        super( pos );
        this.value = value;
        this.typeName = typeName;
        assert typeName != null;
        assert valueMatchesType( value, typeName );
    }


    /**
     * @return whether value is appropriate for its type (we have rules about these things)
     */
    public static boolean valueMatchesType( PolyValue value, PolyType typeName ) {
        return switch ( typeName ) {
            case BOOLEAN -> (value == null) || (value instanceof PolyBoolean);
            case NULL -> value == null;
            case DECIMAL, DOUBLE -> value instanceof PolyNumber;
            case DATE -> value instanceof PolyDate;
            case TIME -> value instanceof PolyTime;
            case TIMESTAMP -> value instanceof PolyTimestamp;
            case INTERVAL -> value instanceof PolyInterval;
            case BINARY -> value instanceof PolyBinary;
            case CHAR -> value instanceof PolyString;
            case SYMBOL -> (value instanceof SqlSampleSpec) || value instanceof PolySymbol;
            case MULTISET -> true;
            case ARRAY -> value instanceof PolyList; // not allowed -- use Decimal
            // not allowed -- use Char
            // not allowed -- use Binary
            default -> throw Util.unexpected( typeName );
        };
    }


    public static SqlNode createArray( List<PolyValue> array, AlgDataType type, ParserPos pos ) {
        return new SqlArrayLiteral( PolyList.copyOf( array ), type, pos );
    }


    public static PolyValue toPoly( SqlNode value ) {
        if ( !(value instanceof SqlLiteral) ) {
            log.warn( "Could not transform: " + value.toString() );
            return null;
        }
        return ((SqlLiteral) value).value;
    }


    @Override
    public SqlLiteral clone( ParserPos pos ) {
        return new SqlLiteral( value, typeName, pos );
    }


    @Override
    public Kind getKind() {
        return Kind.LITERAL;
    }


    public <T> T getValueAs( Class<T> clazz ) {
        if ( clazz.isInstance( value ) ) {
            return clazz.cast( value );
        }
        switch ( typeName ) {
            case CHAR:
                if ( clazz == String.class ) {
                    return clazz.cast( value.asString().getValue() );
                }
                break;
            case BINARY:
                if ( clazz == byte[].class ) {
                    return clazz.cast( value.asBinary().value );
                }
                break;
            case DECIMAL:
                if ( clazz == Long.class ) {
                    return clazz.cast( ((PolyBigDecimal) value).longValue() );
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
                    return clazz.cast( value.asBigDecimal().longValue() );
                } else if ( clazz == Integer.class ) {
                    return clazz.cast( value.asBigDecimal().intValue() );
                } else if ( clazz == Short.class ) {
                    return clazz.cast( value.asBigDecimal().intValue() );
                } else if ( clazz == Byte.class ) {
                    return clazz.cast( value.asBigDecimal().bigDecimalValue().byteValue() );
                } else if ( clazz == Double.class ) {
                    return clazz.cast( (value.asBigDecimal().doubleValue()) );
                } else if ( clazz == Float.class ) {
                    return clazz.cast( value.asBigDecimal().floatValue() );
                }
                break;
            case DATE:
            case TIME:
            case TIMESTAMP:
                if ( clazz == Calendar.class ) {
                    return clazz.cast( value.asTemporal().toCalendar() );
                }
                break;
            case INTERVAL:
                final SqlIntervalLiteral.IntervalValue valMonth = (SqlIntervalLiteral.IntervalValue) value;
                if ( clazz == Long.class ) {
                    return clazz.cast( valMonth.getSign() * SqlParserUtil.intervalToMonths( valMonth ) );
                } else if ( clazz == BigDecimal.class ) {
                    return clazz.cast( BigDecimal.valueOf( getValueAs( Long.class ) ) );
                } else if ( clazz == TimeUnitRange.class ) {
                    return clazz.cast( valMonth.getIntervalQualifier().timeUnitRange );
                }
                break;
            case SYMBOL:
                if ( clazz == Enum.class ) {
                    return clazz.cast( value.asSymbol().value );
                } else if ( clazz == JsonEmptyOrError.class ) {
                    return clazz.cast( value.asSymbol().value );
                }
                break;
        }
        throw new AssertionError( "cannot cast " + value + " as " + clazz );
    }


    /**
     * Returns the value as a symbol.
     */
    @Override
    public <E extends Enum<E>> E symbolValue( Class<E> class_ ) {
        return class_.cast( value.asSymbol().value );
    }


    /**
     * Returns the value as a boolean.
     */
    public boolean booleanValue() {
        return value.asBoolean().value;
    }


    /**
     * Extracts the {@link SqlSampleSpec} value from a symbol literal.
     *
     * @throws ClassCastException if the value is not a symbol literal
     * @see #createSymbol(Enum, ParserPos)
     */
    public static SqlSampleSpec sampleValue( SqlNode node ) {
        return (SqlSampleSpec) ((SqlLiteral) node).value;
    }


    /**
     * Extracts the value from a literal.
     * <p>
     * Cases:
     * <ul>
     * <li>If the node is a character literal, a chain of string literals, or a CAST of a character literal, returns the value as a {@link NlsString}.</li>
     * <li>If the node is a numeric literal, or a negated numeric literal, returns the value as a {@link BigDecimal}.</li>
     * <li>If the node is a {@link SqlIntervalQualifier}, returns its {@link TimeUnitRange}.</li>
     * <li>If the node is INTERVAL_DAY_TIME_ in {@link PolyTypeFamily}, returns its sign multiplied by its millisecond equivalent value</li>
     * <li>If the node is INTERVAL_YEAR_MONTH_ in {@link PolyTypeFamily}, returns its sign multiplied by its months equivalent value</li>
     * <li>Otherwise throws {@link IllegalArgumentException}.</li>
     * </ul>
     */
    public static Comparable<?> value( SqlNode node ) throws IllegalArgumentException {
        if ( node instanceof SqlLiteral literal ) {
            if ( literal.getTypeName() == PolyType.SYMBOL ) {
                return literal.value.asSymbol().value;
            }
            switch ( literal.getTypeName().getFamily() ) {
                case CHARACTER:
                    return literal.value.asString().value;
                case NUMERIC:
                    return literal.value.asNumber().bigDecimalValue();
                case INTERVAL_YEAR_MONTH:
                    final SqlIntervalLiteral.IntervalValue valMonth = (SqlIntervalLiteral.IntervalValue) literal.value;
                    return valMonth.getSign() * SqlParserUtil.intervalToMonths( valMonth );
                case INTERVAL_TIME:
                    final SqlIntervalLiteral.IntervalValue valTime = (SqlIntervalLiteral.IntervalValue) literal.value;
                    return valTime.getSign() * SqlParserUtil.intervalToMillis( valTime );
            }
        }
        if ( SqlUtil.isLiteralChain( node ) ) {
            assert node instanceof SqlCall;
            final SqlLiteral literal = SqlLiteralChainOperator.concatenateOperands( (SqlCall) node );
            assert PolyTypeUtil.inCharFamily( literal.getTypeName() );
            return literal.value.asString().value;
        }
        if ( node instanceof SqlIntervalQualifier qualifier ) {
            return qualifier.timeUnitRange;
        }
        switch ( node.getKind() ) {
            case CAST:
                assert node instanceof SqlCall;
                return value( ((SqlCall) node).operand( 0 ) );
            case MINUS_PREFIX:
                assert node instanceof SqlCall;
                Comparable<?> o = value( ((SqlCall) node).operand( 0 ) );
                if ( o instanceof BigDecimal bigDecimal ) {
                    return bigDecimal.negate();
                }
                // fall through
            default:
                throw new IllegalArgumentException( "not a literal: " + node );
        }
    }


    /**
     * Extracts the string value from a string literal, a chain of string literals, or a CAST of a string literal.
     *
     * @deprecated Use {@link #value(SqlNode)}
     */
    @Deprecated // to be removed before 2.0
    public static String stringValue( SqlNode node ) {
        if ( node instanceof SqlLiteral literal ) {
            assert PolyTypeUtil.inCharFamily( literal.getTypeName() );
            return literal.value.toString();
        } else if ( SqlUtil.isLiteralChain( node ) ) {
            final SqlLiteral literal = SqlLiteralChainOperator.concatenateOperands( (SqlCall) node );
            assert PolyTypeUtil.inCharFamily( literal.getTypeName() );
            return literal.value.toString();
        } else if ( node instanceof SqlCall && ((SqlCall) node).getOperator().getOperatorName() == OperatorName.CAST ) {
            return stringValue( ((SqlCall) node).operand( 0 ) );
        } else {
            throw new AssertionError( "invalid string literal: " + node );
        }
    }


    /**
     * Converts a chained string literals into regular literals; returns regular literals unchanged.
     *
     * @throws IllegalArgumentException if {@code node} is not a string literal and cannot be unchained.
     */
    public static SqlLiteral unchain( SqlNode node ) {
        if ( node instanceof SqlLiteral ) {
            return (SqlLiteral) node;
        } else if ( SqlUtil.isLiteralChain( node ) ) {
            return SqlLiteralChainOperator.concatenateOperands( (SqlCall) node );
        } else if ( node instanceof SqlIntervalQualifier q ) {
            return new SqlLiteral(
                    new SqlIntervalLiteral.IntervalValue( q, PolyInterval.of( 1L, q ) ),
                    q.typeName(),
                    q.pos );
        } else {
            throw new IllegalArgumentException( "invalid literal: " + node );
        }
    }


    /**
     * For calc program builder - value may be different than {@link #unparse}
     * Typical values:
     *
     * <ul>
     * <li>Hello, world!</li>
     * <li>12.34</li>
     * <li>{null}</li>
     * <li>1969-04-29</li>
     * </ul>
     *
     * @return string representation of the value
     */
    @Override
    public String toValue() {
        if ( value == null ) {
            return null;
        }
        if ( Objects.requireNonNull( typeName ) == PolyType.CHAR ) {// We want 'It''s superman!', not _ISO-8859-1'It''s superman!'
            return value.asString().getValue();
        }
        return value.toString();
    }


    @Override
    public void validate( SqlValidator validator, SqlValidatorScope scope ) {
        validator.validateLiteral( this );
    }


    @Override
    public <R> R accept( NodeVisitor<R> visitor ) {
        return visitor.visit( this );
    }


    @Override
    public boolean equalsDeep( Node node, Litmus litmus ) {
        if ( !(node instanceof SqlLiteral that) ) {
            return litmus.fail( "{} != {}", this, node );
        }
        if ( !this.equals( that ) ) {
            return litmus.fail( "{} != {}", this, node );
        }
        return litmus.succeed();
    }


    @Override
    public Monotonicity getMonotonicity( SqlValidatorScope scope ) {
        return Monotonicity.CONSTANT;
    }


    /**
     * Creates a NULL literal.
     * <p>
     * There's no singleton constant for a NULL literal. Instead, nulls must be instantiated via createNull(), because different instances have different context-dependent types.
     */
    public static SqlLiteral createNull( ParserPos pos ) {
        return new SqlLiteral( null, PolyType.NULL, pos );
    }


    /**
     * Creates a boolean literal.
     */
    public static SqlLiteral createBoolean( PolyBoolean b, ParserPos pos ) {
        return createBoolean( b.asBoolean().value, pos );
    }


    public static SqlLiteral createBoolean( boolean b, ParserPos pos ) {
        return b
                ? new SqlLiteral( PolyBoolean.TRUE, PolyType.BOOLEAN, pos )
                : new SqlLiteral( PolyBoolean.FALSE, PolyType.BOOLEAN, pos );
    }


    public static SqlLiteral createUnknown( ParserPos pos ) {
        return new SqlLiteral( null, PolyType.BOOLEAN, pos );
    }


    /**
     * Creates a literal which represents a parser symbol, for example the <code>TRAILING</code> keyword in the call <code>Trim(TRAILING 'x' FROM 'Hello world!')</code>.
     *
     * @see #symbolValue(Class)
     */
    public static SqlLiteral createSymbol( Enum<?> o, ParserPos pos ) {
        return new SqlLiteral( PolySymbol.of( o ), PolyType.SYMBOL, pos );
    }


    /**
     * Creates a literal which represents a sample specification.
     */
    public static SqlLiteral createSample( SqlSampleSpec sampleSpec, ParserPos pos ) {
        return new SqlLiteral( sampleSpec, PolyType.SYMBOL, pos );
    }


    public boolean equals( Object obj ) {
        if ( !(obj instanceof SqlLiteral that) ) {
            return false;
        }
        return Objects.equals( value, that.value );
    }


    public int hashCode() {
        return (value == null) ? 0 : value.hashCode();
    }


    /**
     * Returns the integer value of this literal.
     *
     * @param exact Whether the value has to be exact. If true, and the literal is a fraction (e.g. 3.14), throws. If false, discards the fractional part of the value.
     * @return Integer value of this literal
     */
    @Override
    public int intValue( boolean exact ) {
        switch ( typeName ) {
            case DECIMAL:
            case DOUBLE:
                BigDecimal bd = value.asNumber().BigDecimalValue();
                if ( exact ) {
                    try {
                        return bd.intValueExact();
                    } catch ( ArithmeticException e ) {
                        throw CoreUtil.newContextException( getPos(), Static.RESOURCE.numberLiteralOutOfRange( bd.toString() ) );
                    }
                } else {
                    return bd.intValue();
                }
            default:
                throw Util.unexpected( typeName );
        }
    }


    /**
     * Returns the long value of this literal.
     *
     * @param exact Whether the value has to be exact. If true, and the literal is a fraction (e.g. 3.14), throws. If false, discards the fractional part of the value.
     * @return Long value of this literal
     */
    @Override
    public long longValue( boolean exact ) {
        switch ( typeName ) {
            case DECIMAL:
            case DOUBLE:
                BigDecimal bd = value.asNumber().bigDecimalValue();
                if ( exact ) {
                    try {
                        return bd.longValueExact();
                    } catch ( ArithmeticException e ) {
                        throw CoreUtil.newContextException( getPos(), Static.RESOURCE.numberLiteralOutOfRange( bd.toString() ) );
                    }
                } else {
                    return bd.longValue();
                }
            default:
                throw Util.unexpected( typeName );
        }
    }


    /**
     * Returns sign of value.
     *
     * @return -1, 0 or 1
     */
    @Deprecated // to be removed before 2.0
    public int signum() {
        return bigDecimalValue().compareTo( BigDecimal.ZERO );
    }


    /**
     * Returns a numeric literal's value as a {@link BigDecimal}.
     */
    @Override
    public BigDecimal bigDecimalValue() {
        return switch ( typeName ) {
            case DECIMAL, DOUBLE -> value.asBigDecimal().value;
            default -> throw Util.unexpected( typeName );
        };
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        unparsePoly( typeName, value, writer );
    }


    public static void unparsePoly( PolyType typeName, PolyValue value, SqlWriter writer ) {
        switch ( typeName ) {
            case BOOLEAN:
                writer.keyword(
                        value == null
                                ? "UNKNOWN"
                                : value.asBoolean().value
                                        ? "TRUE"
                                        : "FALSE" );
                break;
            case NULL:
                writer.keyword( "NULL" );
                break;
            case CHAR:
                //case DECIMAL:
            case DOUBLE:
            case BINARY:
                // should be handled in subtype
                throw Util.unexpected( typeName );

            case SYMBOL:
                if ( value.isSymbol() && value.asSymbol().value instanceof TimeUnitRange ) {
                    String s = writer.getDialect().timeUnitName( value.asSymbol().asEnum( TimeUnitRange.class ) );
                    writer.keyword( s );
                } else if ( value.isSymbol() ) {
                    Enum<?> enumVal = value.asSymbol().value;
                    writer.keyword( enumVal.toString() );
                } else {
                    writer.keyword( String.valueOf( value ) );
                }
                break;
            default:
                writer.literal( value.toString() );
        }
    }


    public AlgDataType createSqlType( AlgDataTypeFactory typeFactory ) {
        switch ( typeName ) {
            case NULL:
            case BOOLEAN:
                AlgDataType ret = typeFactory.createPolyType( typeName );
                ret = typeFactory.createTypeWithNullability( ret, null == value );
                return ret;
            case BINARY:
                int bitCount = value.asBinary().getBitCount();
                return typeFactory.createPolyType( PolyType.BINARY, bitCount / 8 );
            case CHAR:
                PolyString string = (PolyString) value;
                Charset charset = string.getCharset();
                if ( null == charset ) {
                    charset = typeFactory.getDefaultCharset();
                }
                Collation collation = Collation.COERCIBLE;//string.getCollation();
                AlgDataType type = typeFactory.createPolyType( PolyType.CHAR, string.getValue().length() );
                type = typeFactory.createTypeWithCharsetAndCollation( type, charset, collation );
                return type;

            case INTERVAL:
                SqlIntervalLiteral.IntervalValue intervalValue = (SqlIntervalLiteral.IntervalValue) value;
                return typeFactory.createIntervalType( intervalValue.getIntervalQualifier() );

            case SYMBOL:
                return typeFactory.createPolyType( PolyType.SYMBOL );

            case INTEGER: // handled in derived class
            case TIME: // handled in derived class
            case VARCHAR: // should never happen
            case VARBINARY: // should never happen

            default:
                throw Util.needToImplement( this + ", operand=" + value );
        }
    }


    public static SqlDateLiteral createDate( PolyDate date, ParserPos pos ) {
        return new SqlDateLiteral( date, pos );
    }


    public static SqlTimestampLiteral createTimestamp( PolyTimestamp ts, int precision, ParserPos pos ) {
        return new SqlTimestampLiteral( ts, precision, false, pos );
    }


    public static SqlTimeLiteral createTime( PolyTime t, int precision, ParserPos pos ) {
        return new SqlTimeLiteral( t, precision, false, pos );
    }


    /**
     * Creates an interval literal.
     *
     * @param intervalQualifier describes the interval type and precision
     * @param pos Parser position
     */
    public static SqlIntervalLiteral createInterval( PolyInterval interval, SqlIntervalQualifier intervalQualifier, ParserPos pos ) {
        return new SqlIntervalLiteral( interval, intervalQualifier, intervalQualifier.typeName(), pos );
    }


    public static SqlNumericLiteral createNegative( SqlNumericLiteral num, ParserPos pos ) {
        return new SqlNumericLiteral(
                ((PolyNumber) num.value).negate(),
                num.getPrec(),
                num.getScale(),
                num.isExact(),
                pos );
    }


    public static SqlNumericLiteral createExactNumeric( String s, ParserPos pos ) {
        BigDecimal value;
        int prec;
        int scale;

        int i = s.indexOf( '.' );
        if ( (i >= 0) && ((s.length() - 1) != i) ) {
            value = CoreUtil.parseDecimal( s );
            scale = s.length() - i - 1;
            assert scale == value.scale() : s;
            prec = s.length() - 1;
        } else if ( (i >= 0) && ((s.length() - 1) == i) ) {
            value = CoreUtil.parseInteger( s.substring( 0, i ) );
            scale = 0;
            prec = s.length() - 1;
        } else {
            value = CoreUtil.parseInteger( s );
            scale = 0;
            prec = s.length();
        }
        return new SqlNumericLiteral( PolyBigDecimal.of( value ), prec, scale, true, pos );
    }


    public static SqlNumericLiteral createApproxNumeric( String s, ParserPos pos ) {
        BigDecimal value = CoreUtil.parseDecimal( s );
        return new SqlNumericLiteral( PolyBigDecimal.of( value ), null, null, false, pos );
    }


    /**
     * Creates a literal like X'ABAB'. Although it matters when we derive a type for this beastie, we don't care at this point whether the number of hexits is odd or even.
     */
    public static SqlBinaryStringLiteral createBinaryString( PolyBinary s, ParserPos pos ) {
        return new SqlBinaryStringLiteral( s, pos );
    }


    public static SqlBinaryStringLiteral createBinaryString( String s, ParserPos pos ) {
        BitString bits;
        try {
            bits = BitString.createFromHexString( s );
        } catch ( NumberFormatException e ) {
            throw CoreUtil.newContextException( pos, Static.RESOURCE.binaryLiteralInvalid() );
        }
        return new SqlBinaryStringLiteral( PolyBinary.of( bits.getAsByteArray(), s.length() ), pos );
    }


    /**
     * Creates a literal like X'ABAB' from an array of bytes.
     *
     * @param bytes Contents of binary literal
     * @param pos Parser position
     * @return Binary string literal
     */
    public static SqlBinaryStringLiteral createBinaryString( byte[] bytes, ParserPos pos ) {
        return new SqlBinaryStringLiteral( PolyBinary.of( bytes ), pos );
    }


    /**
     * Creates a string literal, with optional character-set.
     *
     * @param s a string (without the sql single quotes)
     * @param pos Parser position
     * @return A string literal
     * @throws UnsupportedCharsetException if charSet is not null but there is no character set with that name in this environment
     */
    public static SqlCharStringLiteral createCharString( PolyString s, ParserPos pos ) {
        return new SqlCharStringLiteral( s, pos );
    }


    public static SqlCharStringLiteral createCharString( String s, String charsetName, ParserPos pos ) {
        return new SqlCharStringLiteral( PolyString.of( s, charsetName ), pos );
    }


    public static SqlCharStringLiteral createCharString( String s, ParserPos pos ) {
        return new SqlCharStringLiteral( PolyString.of( s ), pos );
    }


    /**
     * Transforms this literal (which must be of type character) into a new one in which 4-digit Unicode escape sequences have been replaced with the corresponding Unicode characters.
     *
     * @param unicodeEscapeChar escape character (e.g. backslash) for Unicode numeric sequences; 0 implies no transformation
     * @return transformed literal
     */
    public SqlLiteral unescapeUnicode( char unicodeEscapeChar ) {
        if ( unicodeEscapeChar == 0 ) {
            return this;
        }
        assert PolyTypeUtil.inCharFamily( getTypeName() );
        PolyString ns = (PolyString) value;
        String s = ns.getValue();
        StringBuilder sb = new StringBuilder();
        int n = s.length();
        for ( int i = 0; i < n; ++i ) {
            char c = s.charAt( i );
            if ( c == unicodeEscapeChar ) {
                if ( n > (i + 1) ) {
                    if ( s.charAt( i + 1 ) == unicodeEscapeChar ) {
                        sb.append( unicodeEscapeChar );
                        ++i;
                        continue;
                    }
                }
                if ( (i + 5) > n ) {
                    throw CoreUtil.newContextException( getPos(), Static.RESOURCE.unicodeEscapeMalformed( i ) );
                }
                final String u = s.substring( i + 1, i + 5 );
                final int v;
                try {
                    v = Integer.parseInt( u, 16 );
                } catch ( NumberFormatException ex ) {
                    throw CoreUtil.newContextException( getPos(), Static.RESOURCE.unicodeEscapeMalformed( i ) );
                }
                sb.append( (char) (v & 0xFFFF) );

                // skip hexits
                i += 4;
            } else {
                sb.append( c );
            }
        }
        return new SqlCharStringLiteral( PolyString.of( sb.toString(), ns.charset ), getPos() );
    }


    public PolyValue getPolyValue() {
        return value;
    }

}

