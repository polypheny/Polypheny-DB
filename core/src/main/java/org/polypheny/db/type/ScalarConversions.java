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

package org.polypheny.db.type;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.functions.Functions;
import org.polypheny.db.functions.TemporalFunctions;
import org.polypheny.db.type.entity.PolyBinary;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.type.entity.spatial.PolyGeometry;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.type.entity.temporal.PolyTime;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;
import org.polypheny.db.util.temporal.DateTimeUtils;


/**
 * Scalar conversions for resolved values, shared by mapping validation and execution.
 * The class defines which type conversions are supported and perform conversions when a query reads a mapped field
 * */
public final class ScalarConversions {

    private static final Map<PolyType, Set<PolyType>> SUPPORTED_CONVERSIONS = createSupportedConversions();


    private ScalarConversions() {
    }


    public static boolean supports( AlgDataType source, AlgDataType target ) {
        // Both types must be scalar
        // The pair must be supported by the conversion rules
        return isScalar( source ) && isScalar( target ) && supports( source.getPolyType(), target.getPolyType() );
    }


    public static List<PolyType> targetTypes( AlgDataType source ) {
        if ( !isScalar( source ) ) {
            return List.of();
        }
        return List.copyOf( SUPPORTED_CONVERSIONS.get( source.getPolyType() ) );
    }


    private static boolean isScalar( AlgDataType type ) {
        return !type.isStruct() && type.getComponentType() == null && type.getPolyType() != null;
    }


    /**
     * The pair must be supported by the conversion rules
     */
    private static boolean supports( PolyType source, PolyType target ) {
        return SUPPORTED_CONVERSIONS.get( source ).contains( target );
    }


    private static Map<PolyType, Set<PolyType>> createSupportedConversions() {
        Map<PolyType, Set<PolyType>> conversions = new EnumMap<>( PolyType.class );
        // different types conversion
        // define valid source type for target type
        Set<PolyType> textTypes = EnumSet.of( PolyType.CHAR, PolyType.VARCHAR, PolyType.TEXT );
        Set<PolyType> numericTargets = EnumSet.copyOf( PolyType.NUMERIC_TYPES ); // type we want to convert to
        numericTargets.addAll( textTypes );
        numericTargets.add( PolyType.BOOLEAN );
        PolyType.NUMERIC_TYPES.forEach( source -> conversions.put( source, numericTargets ) );
        conversions.put( PolyType.BOOLEAN, numericTargets );

        Set<PolyType> textTargets = EnumSet.copyOf( numericTargets );
        textTargets.addAll( EnumSet.of( PolyType.DATE, PolyType.TIME, PolyType.TIMESTAMP,
                PolyType.BINARY, PolyType.VARBINARY, PolyType.JSON, PolyType.GEOMETRY ) );
        textTypes.forEach( source -> conversions.put( source, textTargets ) );

        Set<PolyType> dateTimeTargets = EnumSet.copyOf( textTypes );
        dateTimeTargets.add( PolyType.TIMESTAMP );
        conversions.put( PolyType.DATE, dateTimeTargets );
        conversions.put( PolyType.TIME, dateTimeTargets );
        conversions.put( PolyType.TIMESTAMP, EnumSet.of( PolyType.CHAR, PolyType.VARCHAR, PolyType.TEXT, PolyType.DATE, PolyType.TIME ) );
        conversions.put( PolyType.JSON, textTypes );
        conversions.put( PolyType.GEOMETRY, textTypes );

        Set<PolyType> binaryTargets = EnumSet.copyOf( textTypes );
        binaryTargets.addAll( PolyType.BINARY_TYPES );
        PolyType.BINARY_TYPES.forEach( source -> conversions.put( source, binaryTargets ) );
        PolyType.BLOB_TYPES.forEach( source -> conversions.put( source, EnumSet.of( PolyType.FILE ) ) );

        // Same-type conversions are supported even when the value can only be passed through.
        for ( PolyType source : PolyType.values() ) {
            Set<PolyType> targets = EnumSet.of( source );
            targets.addAll( conversions.getOrDefault( source, Set.of() ) );
            conversions.put( source, Collections.unmodifiableSet( targets ) );
        }
        return Collections.unmodifiableMap( conversions );
    }


    private static boolean isText( PolyType type ) {
        return type == PolyType.CHAR || type == PolyType.VARCHAR || type == PolyType.TEXT;
    }


    public static PolyValue convert( PolyValue value, PolyType source, PolyType target, int precision, int scale, DataContext context ) {
        if ( isNull( value ) ) {
            return null;
        }
        if ( !supports( source, target ) ) {
            throw new IllegalArgumentException( "Unsupported scalar conversion from " + source + " to " + target );
        }
        return switch ( target ) {
            case TINYINT -> PolyInteger.of( integer( value, source ).byteValueExact() );
            case SMALLINT -> PolyInteger.of( integer( value, source ).shortValueExact() );
            case INTEGER -> PolyInteger.of( integer( value, source ).intValueExact() );
            case BIGINT -> PolyLong.of( integer( value, source ).longValueExact() );
            case DECIMAL -> decimal( value, source, precision, scale );
            case REAL, FLOAT -> toFloat( value, source );
            case DOUBLE -> toDouble( value, source );
            case CHAR, VARCHAR, TEXT -> text( value, source, target, precision );
            case BINARY, VARBINARY -> binary( value, source, target, precision );
            case BOOLEAN -> switch ( source.getFamily() ) {
                case BOOLEAN -> value;
                case NUMERIC -> PolyBoolean.of( value.asNumber().doubleValue() != 0 );
                default -> Functions.toBoolean( value.asString() );
            };
            case JSON -> source == target ? value : json( value.asString() );
            case GEOMETRY -> source == target ? value : PolyGeometry.ofOrThrow( value.asString().value );
            case DATE -> switch ( source ) {
                case DATE -> value;
                case TIMESTAMP -> PolyDate.of( value.asTimestamp().getMillisSinceEpoch() );
                default -> PolyDate.ofDays( Math.toIntExact( LocalDate.parse( value.asString().value.trim() ).toEpochDay() ) );
            };
            case TIME -> time( value, source, precision );
            case TIMESTAMP -> timestamp( value, source, precision, context );
            case FILE, IMAGE, VIDEO, AUDIO -> value;
            // The support check above only lets same-type pairs reach this branch.
            default -> value;
        };
    }


    public static boolean isNull( PolyValue value ) {
        if ( value == null || value.isNull() ) {
            return true;
        }
        return switch ( value.type ) {
            case BIGINT -> value.asLong().value == null;
            case REAL, FLOAT -> value.asFloat().value == null;
            case DOUBLE -> value.asDouble().value == null;
            case DATE, TIME, TIMESTAMP -> value.asTemporal().getMillisSinceEpoch() == null;
            case BINARY, VARBINARY -> value.asBinary().value == null;
            default -> false;
        };
    }


    private static BigInteger integer( PolyValue value, PolyType source ) {
        if ( isText( source ) ) {
            String text = value.asString().value;
            return new BigInteger( source == PolyType.CHAR ? text.trim() : text );
        }
        return number( value, source ).toBigInteger();
    }


    private static BigDecimal number( PolyValue value, PolyType source ) {
        if ( source == PolyType.BOOLEAN ) {
            return value.asBoolean().value ? BigDecimal.ONE : BigDecimal.ZERO;
        }
        return isText( source ) ? new BigDecimal( value.asString().value.trim() ) : value.asNumber().bigDecimalValue();
    }


    private static PolyBigDecimal decimal( PolyValue value, PolyType source, int precision, int scale ) {
        BigDecimal rounded = number( value, source ).setScale( scale, RoundingMode.HALF_UP );
        if ( rounded.precision() > precision ) {
            throw new ArithmeticException( "Decimal precision exceeded" );
        }
        return PolyBigDecimal.of( rounded );
    }


    private static PolyFloat toFloat( PolyValue value, PolyType source ) {
        float result = isText( source ) ? Float.parseFloat( value.asString().value )
                : source == PolyType.BOOLEAN ? number( value, source ).floatValue() : value.asNumber().floatValue();
        if ( !Float.isFinite( result ) ) {
            throw new ArithmeticException( "Non-finite floating-point result" );
        }
        return PolyFloat.of( result );
    }


    private static PolyDouble toDouble( PolyValue value, PolyType source ) {
        double result = isText( source ) ? Double.parseDouble( value.asString().value )
                : source == PolyType.BOOLEAN ? number( value, source ).doubleValue() : value.asNumber().doubleValue();
        if ( !Double.isFinite( result ) ) {
            throw new ArithmeticException( "Non-finite floating-point result" );
        }
        return PolyDouble.of( result );
    }


    private static PolyString text( PolyValue value, PolyType source, PolyType target, int precision ) {
        PolyString string = switch ( source ) {
            case CHAR, VARCHAR, TEXT, JSON -> value.asString();
            case BOOLEAN -> PolyString.of( value.asBoolean().value ? "TRUE" : "FALSE" );
            case BINARY, VARBINARY -> PolyString.of( HexFormat.of().formatHex( value.asBinary().value ) );
            case GEOMETRY -> PolyString.of( value.asGeometry().toWKT() );
            default -> PolyString.of( value.toJson() );
        };
        if ( precision < 0 ) {
            return string;
        }
        return target == PolyType.CHAR ? Functions.truncateOrPad( string, precision ) : Functions.truncate( string, precision );
    }


    private static PolyBinary binary( PolyValue value, PolyType source, PolyType target, int precision ) {
        PolyBinary binary = isText( source ) ? PolyBinary.of( HexFormat.of().parseHex( value.asString().value.trim() ) ) : value.asBinary();
        if ( precision < 0 ) {
            return binary;
        }
        if ( target == PolyType.BINARY ) {
            return PolyBinary.of( Arrays.copyOf( binary.value, precision ) );
        }
        return Functions.truncate( binary, precision );
    }


    private static PolyString json( PolyString value ) {
        try {
            if ( PolyValue.JSON_WRAPPER.reader().with( DeserializationFeature.FAIL_ON_TRAILING_TOKENS ).readTree( value.value ).isMissingNode() ) {
                throw new IllegalArgumentException( "Expected a JSON value" );
            }
            return value;
        } catch ( JsonProcessingException e ) {
            throw new IllegalArgumentException( "Invalid JSON value", e );
        }
    }


    @SuppressWarnings("DataFlowIssue")
    private static PolyTime time( PolyValue value, PolyType source, int precision ) {
        long millis = switch ( source ) {
            case TIME -> value.asTime().getMillisSinceEpoch();
            case TIMESTAMP -> Math.floorMod( value.asTimestamp().getMillisSinceEpoch(), DateTimeUtils.MILLIS_PER_DAY );
            default -> LocalTime.parse( value.asString().value.trim() ).toNanoOfDay() / 1_000_000;
        };
        return PolyTime.of( (int) truncateMillis( millis, precision ) );
    }


    @SuppressWarnings("DataFlowIssue")
    private static PolyTimestamp timestamp( PolyValue value, PolyType source, int precision, DataContext context ) {
        long millis = switch ( source ) {
            case TIMESTAMP -> value.asTimestamp().getMillisSinceEpoch();
            case DATE -> value.asDate().getMillisSinceEpoch();
            case TIME -> TemporalFunctions.currentDate( context ).getMillisSinceEpoch() + value.asTime().getMillisSinceEpoch();
            default -> LocalDateTime.parse( value.asString().value.trim().replace( ' ', 'T' ) ).toInstant( ZoneOffset.UTC ).toEpochMilli();
        };
        return PolyTimestamp.of( truncateMillis( millis, precision ) );
    }


    private static long truncateMillis( long millis, int precision ) {
        long unit = switch ( precision ) {
            case 0 -> 1000;
            case 1 -> 100;
            case 2 -> 10;
            default -> 1;
        };
        return millis - Math.floorMod( millis, unit );
    }

}
