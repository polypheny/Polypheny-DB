/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.functions;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.runtime.SqlStateException;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.temporal.PolyCypherTemporal;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyLong;


/** Cypher scalar functions whose argument and result semantics differ from the corresponding SQL functions. */
public enum CypherScalarFunction {
    LEFT( "left", 2, PolyType.VARCHAR ),
    RIGHT( "right", 2, PolyType.VARCHAR ),
    LTRIM( "ltrim", 1, PolyType.VARCHAR ),
    RTRIM( "rtrim", 1, PolyType.VARCHAR ),
    TRIM( "trim", 1, PolyType.VARCHAR ),
    REPLACE( "replace", 3, PolyType.VARCHAR ),
    REVERSE( "reverse", 1, PolyType.ANY ),
    HEAD( "head", 1, PolyType.ANY ),
    TAIL( "tail", 1, PolyType.ARRAY ),
    SPLIT( "split", 2, PolyType.ARRAY ),
    SUBSTRING( "substring", 2, 3, PolyType.VARCHAR ),
    TO_LOWER( "toLower", 1, PolyType.VARCHAR ),
    TO_UPPER( "toUpper", 1, PolyType.VARCHAR ),
    TO_STRING( "toString", 1, PolyType.VARCHAR ),
    ABS( "abs", 1, PolyType.ANY ),
    CEIL( "ceil", 1, PolyType.DOUBLE ),
    FLOOR( "floor", 1, PolyType.DOUBLE ),
    RAND( "rand", 0, PolyType.DOUBLE ),
    ROUND( "round", 1, PolyType.DOUBLE ),
    SIGN( "sign", 1, PolyType.BIGINT ),
    E( "e", 0, PolyType.DOUBLE ),
    EXP( "exp", 1, PolyType.DOUBLE ),
    LOG( "log", 1, PolyType.DOUBLE ),
    LOG10( "log10", 1, PolyType.DOUBLE ),
    SQRT( "sqrt", 1, PolyType.DOUBLE ),
    ACOS( "acos", 1, PolyType.DOUBLE ),
    ASIN( "asin", 1, PolyType.DOUBLE ),
    ATAN( "atan", 1, PolyType.DOUBLE ),
    ATAN2( "atan2", 2, PolyType.DOUBLE ),
    COS( "cos", 1, PolyType.DOUBLE ),
    COT( "cot", 1, PolyType.DOUBLE ),
    DEGREES( "degrees", 1, PolyType.DOUBLE ),
    PI( "pi", 0, PolyType.DOUBLE ),
    RADIANS( "radians", 1, PolyType.DOUBLE ),
    SIN( "sin", 1, PolyType.DOUBLE ),
    TAN( "tan", 1, PolyType.DOUBLE ),
    TO_BOOLEAN( "toBoolean", 1, PolyType.BOOLEAN ),
    TO_INTEGER( "toInteger", 1, PolyType.BIGINT ),
    TO_FLOAT( "toFloat", 1, PolyType.DOUBLE );

    private static final Map<String, CypherScalarFunction> NAMES = Arrays.stream( values() )
            .collect( Collectors.toUnmodifiableMap( function -> function.name.toLowerCase( Locale.ROOT ), Function.identity() ) );
    private static final BigDecimal HALF = new BigDecimal( "0.5" );
    private static final BigDecimal INTEGER_LOWER_LIMIT = BigDecimal.valueOf( Long.MIN_VALUE ).subtract( BigDecimal.ONE );
    private static final BigDecimal INTEGER_UPPER_LIMIT = BigDecimal.valueOf( Long.MAX_VALUE ).add( BigDecimal.ONE );

    private final String name;
    private final int minimumArity;
    private final int maximumArity;
    private final PolyType returnType;


    CypherScalarFunction( String name, int arity, PolyType returnType ) {
        this( name, arity, arity, returnType );
    }


    CypherScalarFunction( String name, int minimumArity, int maximumArity, PolyType returnType ) {
        this.name = name;
        this.minimumArity = minimumArity;
        this.maximumArity = maximumArity;
        this.returnType = returnType;
    }


    @Nullable
    public static CypherScalarFunction named( String name ) {
        String normalized = name.toLowerCase( Locale.ROOT );
        return switch ( normalized ) {
            case "lower" -> TO_LOWER;
            case "upper" -> TO_UPPER;
            default -> NAMES.get( normalized );
        };
    }


    @Nullable
    public String arityError( int arity ) {
        if ( arity >= minimumArity && arity <= maximumArity ) {
            return null;
        }
        if ( minimumArity == maximumArity ) {
            return name + "() requires exactly " + minimumArity + (minimumArity == 1 ? " argument." : " arguments.");
        }
        return name + "() requires " + minimumArity + " or " + maximumArity + " arguments.";
    }


    public AlgDataType resultType( AlgDataTypeFactory factory, List<AlgDataType> operands ) {
        if ( this == HEAD || this == TAIL ) {
            AlgDataType component = operands.get( 0 ).getComponentType();
            AlgDataType element = component == null ? factory.createPolyType( PolyType.ANY ) : component;
            return this == HEAD ? element : factory.createArrayType( element, -1, 1 );
        }
        if ( this == REVERSE ) {
            AlgDataType operandType = operands.get( 0 );
            if ( operandType.getPolyType() == PolyType.ARRAY ) {
                return operandType;
            }
            if ( PolyType.CHAR_TYPES.contains( operandType.getPolyType() ) ) {
                return factory.createPolyType( PolyType.VARCHAR );
            }
        }
        if ( this == ABS ) {
            AlgDataType operandType = operands.get( 0 );
            PolyType type = operandType.getPolyType();
            if ( PolyType.INT_TYPES.contains( type ) ) {
                return factory.createPolyType( PolyType.BIGINT );
            }
            if ( PolyType.APPROX_TYPES.contains( type ) ) {
                return factory.createPolyType( PolyType.DOUBLE );
            }
            if ( type == PolyType.DECIMAL ) {
                return operandType;
            }
        }
        return returnType == PolyType.ARRAY
                ? factory.createArrayType( factory.createPolyType( PolyType.VARCHAR ), -1, 1 )
                : factory.createPolyType( returnType );
    }


    public static PolyValue evaluate( PolyString function, PolyValue[] arguments ) {
        return valueOf( function.value ).apply( arguments );
    }


    // The hidden call site keeps independent rand() calls distinct during expression sharing.
    public static PolyDouble random( PolyString callSite ) {
        return PolyDouble.of( ThreadLocalRandom.current().nextDouble() );
    }


    public PolyValue apply( PolyValue[] arguments ) {
        String arityError = arityError( arguments.length );
        if ( arityError != null ) {
            throw new GenericRuntimeException( arityError );
        }
        if ( arguments.length > 0 && isNull( arguments[0] ) ) {
            return null;
        }

        return switch ( this ) {
            case LEFT, RIGHT, SUBSTRING -> slice( arguments );
            case LTRIM -> PolyString.of( requireString( arguments[0], 1 ).value.stripLeading() );
            case RTRIM -> PolyString.of( requireString( arguments[0], 1 ).value.stripTrailing() );
            case TRIM -> PolyString.of( requireString( arguments[0], 1 ).value.strip() );
            case REPLACE -> isNull( arguments[1] ) || isNull( arguments[2] ) ? null : replace( arguments );
            case REVERSE -> reverse( arguments[0] );
            case HEAD -> head( arguments[0] );
            case TAIL -> tail( arguments[0] );
            case SPLIT -> isNull( arguments[1] ) ? null : split( arguments );
            case TO_LOWER -> Functions.lower( requireString( arguments[0], 1 ) );
            case TO_UPPER -> Functions.upper( requireString( arguments[0], 1 ) );
            case TO_STRING -> stringValue( arguments[0] );
            case ABS -> absolute( requireNumber( arguments[0], 1 ) );
            case CEIL, FLOOR -> integerBoundary( requireNumber( arguments[0], 1 ) );
            case RAND -> random( null );
            case ROUND -> round( requireNumber( arguments[0], 1 ) );
            case SIGN -> sign( requireNumber( arguments[0], 1 ) );
            case E -> PolyDouble.of( Math.E );
            case EXP -> PolyDouble.of( Math.exp( requireNumber( arguments[0], 1 ).doubleValue() ) );
            case LOG, LOG10 -> logarithm( requireNumber( arguments[0], 1 ).doubleValue() );
            case SQRT -> squareRoot( requireNumber( arguments[0], 1 ).doubleValue() );
            case ACOS, ASIN -> inverseCircular( requireNumber( arguments[0], 1 ) );
            case ATAN -> Functions.atan( requireNumber( arguments[0], 1 ) );
            case ATAN2 -> isNull( arguments[1] ) ? null : Functions.atan2( requireNumber( arguments[0], 1 ), requireNumber( arguments[1], 2 ) );
            case COS -> Functions.cos( requireNumber( arguments[0], 1 ) );
            case COT -> cotangent( requireNumber( arguments[0], 1 ) );
            case DEGREES -> Functions.degrees( requireNumber( arguments[0], 1 ) );
            case PI -> PolyDouble.of( Math.PI );
            case RADIANS -> Functions.radians( requireNumber( arguments[0], 1 ) );
            case SIN -> Functions.sin( requireNumber( arguments[0], 1 ) );
            case TAN -> Functions.tan( requireNumber( arguments[0], 1 ) );
            case TO_BOOLEAN -> toBoolean( arguments[0] );
            case TO_INTEGER -> toInteger( arguments[0] );
            case TO_FLOAT -> toFloat( arguments[0] );
        };
    }


    private PolyString slice( PolyValue[] arguments ) {
        String original = requireString( arguments[0], 1 ).value;
        int size = original.codePointCount( 0, original.length() );
        int start;
        int length;
        if ( this == SUBSTRING ) {
            start = clampedBound( arguments[1], "start", 0, size );
            length = arguments.length == 3 ? clampedBound( arguments[2], "length", 0, size - start ) : size - start;
        } else {
            length = clampedBound( arguments[1], "length", 1, size );
            start = this == RIGHT ? size - length : 0;
        }
        int beginOffset = original.offsetByCodePoints( 0, start );
        int endOffset = original.offsetByCodePoints( beginOffset, length );
        return PolyString.of( original.substring( beginOffset, endOffset ) );
    }


    private int clampedBound( PolyValue value, String argument, int minimum, int maximum ) {
        String type = minimum == 0 ? "non-negative integer" : "positive integer";
        if ( isNull( value ) || !PolyType.INT_TYPES.contains( value.getType() ) || value.asNumber().longValue() < minimum ) {
            throw new GenericRuntimeException( name + "() requires a " + type + " " + argument + "." );
        }
        return (int) Math.min( value.asNumber().longValue(), maximum );
    }


    private PolyString replace( PolyValue[] arguments ) {
        String original = requireString( arguments[0], 1 ).value;
        String search = requireString( arguments[1], 2 ).value;
        String replacement = requireString( arguments[2], 3 ).value;
        if ( !search.isEmpty() || replacement.isEmpty() ) {
            return PolyString.of( original.replace( search, replacement ) );
        }
        // Empty matches lie at code-point boundaries, including either end of the string.
        StringBuilder result = new StringBuilder( replacement );
        original.codePoints().forEach( point -> result.appendCodePoint( point ).append( replacement ) );
        return PolyString.of( result.toString() );
    }


    private PolyValue reverse( PolyValue value ) {
        if ( value.isList() ) {
            List<PolyValue> reversed = new ArrayList<>( value.asList() );
            Collections.reverse( reversed );
            return PolyList.copyOf( reversed );
        }
        if ( value.isString() ) {
            return PolyString.of( new StringBuilder( value.asString().value ).reverse().toString() );
        }
        throw new GenericRuntimeException( "reverse() requires a string or list." );
    }


    private PolyValue head( PolyValue value ) {
        PolyList<?> list = requireList( value );
        PolyValue first = list.isEmpty() ? null : list.get( 0 );
        return isNull( first ) ? null : first;
    }


    private PolyList<?> tail( PolyValue value ) {
        PolyList<?> list = requireList( value );
        return list.isEmpty() ? PolyList.EMPTY_LIST : PolyList.copyOf( list.subList( 1, list.size() ) );
    }


    private PolyList<?> requireList( PolyValue value ) {
        if ( value instanceof PolyList<?> list ) {
            return list;
        }
        throw new GenericRuntimeException( name + "() requires a list." );
    }


    private PolyList<PolyString> split( PolyValue[] arguments ) {
        String original = requireString( arguments[0], 1 ).value;
        String delimiter = requireString( arguments[1], 2 ).value;
        if ( delimiter.isEmpty() ) {
            return PolyList.copyOf( original.codePoints().mapToObj( point -> PolyString.of( Character.toString( point ) ) ).toList() );
        }
        List<PolyString> parts = new ArrayList<>();
        int start = 0;
        for ( int match = original.indexOf( delimiter ); match != -1; match = original.indexOf( delimiter, start ) ) {
            parts.add( PolyString.of( original.substring( start, match ) ) );
            start = match + delimiter.length();
        }
        parts.add( PolyString.of( original.substring( start ) ) );
        return PolyList.copyOf( parts );
    }


    private PolyString stringValue( PolyValue value ) {
        if ( value.isString() ) {
            return value.asString();
        }
        PolyCypherTemporal temporal = PolyCypherTemporal.from( value );
        if ( temporal != null ) {
            return PolyString.of( temporal.toString() );
        }
        if ( PolyType.NUMERIC_TYPES.contains( value.getType() ) || value.isBoolean() ) {
            return PolyString.of( value.toString() );
        }
        throw invalidConversionArgument( "toString() requires a string, number, boolean, or temporal value." );
    }


    private static PolyNumber absolute( PolyNumber value ) {
        if ( PolyType.INT_TYPES.contains( value.getType() ) ) {
            long integer = value.longValue();
            if ( integer == Long.MIN_VALUE ) {
                throw new GenericRuntimeException( "abs() integer result is out of range." );
            }
            return PolyLong.of( Math.abs( integer ) );
        }
        return value.getType() == PolyType.DECIMAL ? Functions.abs( value ) : PolyDouble.of( Math.abs( value.doubleValue() ) );
    }


    private PolyDouble integerBoundary( PolyNumber value ) {
        if ( PolyType.APPROX_TYPES.contains( value.getType() ) ) {
            double operand = value.doubleValue();
            return PolyDouble.of( this == CEIL ? Math.ceil( operand ) : Math.floor( operand ) );
        }
        PolyNumber result = this == CEIL ? Functions.ceil( value ) : Functions.floor( value );
        return PolyDouble.of( result.doubleValue() );
    }


    private static PolyDouble round( PolyNumber value ) {
        if ( PolyType.APPROX_TYPES.contains( value.getType() ) ) {
            double operand = value.doubleValue();
            // At this magnitude every finite double is already an integer. Avoid saturating to a long.
            return PolyDouble.of( Math.abs( operand ) < 0x1.0p52 ? (double) Math.round( operand ) : operand );
        }
        return PolyDouble.of( value.bigDecimalValue().add( HALF ).setScale( 0, RoundingMode.FLOOR ).doubleValue() );
    }


    private static PolyLong sign( PolyNumber value ) {
        if ( PolyType.APPROX_TYPES.contains( value.getType() ) ) {
            double operand = value.doubleValue();
            return Double.isNaN( operand ) ? null : PolyLong.of( (long) Math.signum( operand ) );
        }
        return PolyLong.of( value.bigDecimalValue().signum() );
    }


    private PolyDouble logarithm( double operand ) {
        return operand == 0 ? null : PolyDouble.of( this == LOG ? Math.log( operand ) : Math.log10( operand ) );
    }


    private static PolyDouble squareRoot( double operand ) {
        return operand < 0 ? null : PolyDouble.of( Math.sqrt( operand ) );
    }


    private PolyNumber inverseCircular( PolyNumber value ) {
        double operand = value.doubleValue();
        if ( operand < -1 || operand > 1 ) {
            return null;
        }
        return this == ACOS ? Functions.acos( value ) : Functions.asin( value );
    }


    private static PolyNumber cotangent( PolyNumber value ) {
        return value.doubleValue() == 0 ? null : Functions.cot( value );
    }


    private static PolyBoolean toBoolean( PolyValue value ) {
        if ( value.isBoolean() ) {
            return value.asBoolean();
        }
        if ( value.isString() ) {
            return switch ( value.asString().value.toLowerCase( Locale.ROOT ) ) {
                case "true" -> PolyBoolean.TRUE;
                case "false" -> PolyBoolean.FALSE;
                default -> null;
            };
        }
        throw invalidConversionArgument( "toBoolean() requires a string or boolean." );
    }


    private static PolyLong toInteger( PolyValue value ) {
        BigDecimal number;
        if ( value.isString() ) {
            try {
                number = new BigDecimal( value.asString().value.strip() );
            } catch ( NumberFormatException ex ) {
                return null;
            }
        } else if ( PolyType.NUMERIC_TYPES.contains( value.getType() ) ) {
            PolyNumber operand = value.asNumber();
            if ( PolyType.APPROX_TYPES.contains( operand.getType() ) ) {
                double floating = operand.doubleValue();
                if ( !Double.isFinite( floating ) || floating < -0x1.0p63 || floating >= 0x1.0p63 ) {
                    return null;
                }
                return PolyLong.of( (long) floating );
            }
            number = operand.bigDecimalValue();
        } else {
            throw invalidConversionArgument( "toInteger() requires a string or number." );
        }
        // Check before truncating to avoid expanding a string with a very large exponent.
        if ( number.compareTo( INTEGER_LOWER_LIMIT ) <= 0 || number.compareTo( INTEGER_UPPER_LIMIT ) >= 0 ) {
            return null;
        }
        return PolyLong.of( number.longValue() );
    }


    private static PolyDouble toFloat( PolyValue value ) {
        if ( value.isString() ) {
            try {
                return PolyDouble.of( Double.parseDouble( value.asString().value ) );
            } catch ( NumberFormatException ex ) {
                return null;
            }
        }
        if ( PolyType.NUMERIC_TYPES.contains( value.getType() ) ) {
            return PolyDouble.of( value.asNumber().doubleValue() );
        }
        throw invalidConversionArgument( "toFloat() requires a string or number." );
    }


    private static SqlStateException invalidConversionArgument( String message ) {
        return new SqlStateException( "InvalidArgumentValue: " + message, "22023" );
    }


    private PolyNumber requireNumber( PolyValue value, int argument ) {
        if ( !PolyType.NUMERIC_TYPES.contains( value.getType() ) ) {
            throw new GenericRuntimeException( name + "() requires a number for argument " + argument + "." );
        }
        return value.asNumber();
    }


    private PolyString requireString( PolyValue value, int argument ) {
        if ( !value.isString() ) {
            throw new GenericRuntimeException( name + "() requires a string for argument " + argument + "." );
        }
        return value.asString();
    }


    private static boolean isNull( PolyValue value ) {
        return value == null || value.isNull();
    }

}
