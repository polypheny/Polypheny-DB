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

package org.polypheny.db.adapter.parquet.shared.schema;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.EnumLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.JsonLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.type.entity.PolyBinary;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.type.entity.temporal.PolyTime;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;
import org.polypheny.db.util.temporal.DateTimeUtils;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.Objects;

import static io.activej.common.StringFormatUtils.parseLocalDateTime;

/**
 * converts Parquet schema types and runtime values into the Polypheny type system
 */
public class ParquetTypeConverter {

    /**
     * Convert original parquet type into {@link PolyType}
     *
     * @param field parquet type
     * @return {@link PolyType}
     */
    public PolyType fromParquetTypeToPolyType( Type field ) {

        // treat nested types as document
        if ( !field.isPrimitive() ) {
            return PolyType.DOCUMENT;
        }

        // not nested types
        PrimitiveType primitive = field.asPrimitiveType();
        LogicalTypeAnnotation logical = primitive.getLogicalTypeAnnotation();

        // date types
        if ( logical instanceof DateLogicalTypeAnnotation ) {
            return PolyType.DATE;
        }
        if ( logical instanceof TimeLogicalTypeAnnotation ) {
            return PolyType.TIME;
        }
        if ( logical instanceof TimestampLogicalTypeAnnotation ) {
            return PolyType.TIMESTAMP;
        }

        // string like types
        if ( logical instanceof StringLogicalTypeAnnotation
                || logical instanceof EnumLogicalTypeAnnotation
                || logical instanceof JsonLogicalTypeAnnotation ) {
            return PolyType.TEXT;
        }

        // primitive types - existing in parquet
        return switch ( primitive.getPrimitiveTypeName() ) {
            case BOOLEAN -> PolyType.BOOLEAN;
            case INT32 -> PolyType.INTEGER;
            case INT64 -> PolyType.BIGINT;
            case FLOAT -> PolyType.FLOAT;
            case DOUBLE -> PolyType.DOUBLE;
            case FIXED_LEN_BYTE_ARRAY, BINARY, INT96 -> PolyType.VARBINARY;
        };
    }


    /**
     * Convert value from object to appropriate PolyValue
     *
     * @param fieldType - type of value/column
     * @param value - object to convert
     * @return converted PolyValue
     */
    public PolyValue fromObjToPolyValue( Type fieldType, Object value ) {
        if ( value == null ) {
            return PolyNull.NULL;
        }

        var polyType = fromParquetTypeToPolyType( fieldType );

        return switch ( polyType ) {
            case BOOLEAN -> fromObjToPolyBoolean( value );
            case VARBINARY -> fromObjToPolyBinary( value );
            case INTEGER -> fromObjToPolyInteger( value );
            case BIGINT -> fromObjToPolyLong( value );
            case FLOAT -> fromObjToPolyFloat( value );
            case DOUBLE -> fromObjToPolyDouble( value );
            case DATE -> fromObjToPolyDate( value );
            case TIME -> fromObjToPolyTime( value );
            case TIMESTAMP -> fromObjToPolyTimestamp( value );
            case VARCHAR, TEXT -> fromObjToPolyString( value );
            default -> throw new IllegalStateException( "Unexpected value: " + fieldType );
        };
    }


    /**
     * Convert a typed literal to a parquet primitive value.
     */
    public Object fromPolyValueToParquetObj( PrimitiveType primitive, PolyValue literal ) {
        if ( literal == null || literal.isNull() ) {
            return null;
        }

        try {
            Object date = fromPolyValueToDate( primitive, literal );
            if ( date != null ) {
                return date;
            }

            return switch ( primitive.getPrimitiveTypeName() ) {
                case BOOLEAN -> literal.isBoolean() ? literal.asBoolean().value : Boolean.parseBoolean( literal.toString() );
                case INT32 -> literal.asNumber().intValue();
                case INT64 -> literal.asNumber().longValue();
                case FLOAT -> literal.asNumber().floatValue();
                case DOUBLE -> literal.asNumber().doubleValue();
                case INT96 -> null;
                case FIXED_LEN_BYTE_ARRAY, BINARY -> literal.isBinary()
                        ? Binary.fromConstantByteArray( literal.asBinary().value )
                        : Binary.fromString( literal.isString() ? literal.asString().value : literal.toString() );
            };
        } catch ( Exception e ) {
            return null;
        }
    }


    /**
     * Convert PolyValue to PolyString
     *
     * @param value - to convert
     * @return PolyString
     */
    public PolyString valueToPolyString( PolyValue value ) {
        return switch ( value.getType() ) {
            case NULL -> PolyString.of( null );
            case ANY, DOCUMENT, GRAPH, NODE, EDGE, PATH, ARRAY, MAP -> value.toPolyJson();
            default -> PolyString.convert( value );
        };
    }


    /**
     * Converts a raw Parquet footer statistic value into the compact string representation
     * stored in {@code ParquetColumnStatistics}.
     */
    public String fromParquetValueToString( PrimitiveType primitive, Object value ) {
        if ( value == null ) {
            return null;
        }

        PolyType polyType = fromParquetTypeToPolyType( primitive );
        if ( polyType == PolyType.DATE && value instanceof Number number ) {
            return String.valueOf( number.longValue() );
        }
        if ( polyType == PolyType.TIME && value instanceof Number number ) {
            return String.valueOf( normalizeParquetTimeToMillis( primitive, number.longValue() ) );
        }
        if ( polyType == PolyType.TIMESTAMP && value instanceof Number number ) {
            return String.valueOf( normalizeParquetTimestampToMillis( primitive, number.longValue() ) );
        }
        if ( value instanceof Binary binary ) {
            return binary.toStringUsingUTF8();
        }
        return String.valueOf( value );
    }


    /**
     * Converts a string value into a PolyValue.
     */
    public PolyValue fromStringToPolyValue( PolyType polyType, String value ) {
        if ( value == null ) {
            return null;
        }
        try {
            return switch ( polyType ) {
                case BOOLEAN -> PolyBoolean.of( Boolean.parseBoolean( value ) );
                case TINYINT, SMALLINT, INTEGER -> PolyInteger.of( Integer.parseInt( value ) );
                case BIGINT -> PolyLong.of( Long.parseLong( value ) );
                case FLOAT, REAL -> PolyFloat.of( Float.parseFloat( value ) );
                case DOUBLE -> PolyDouble.of( Double.parseDouble( value ) );
                case DATE -> PolyDate.of( Long.parseLong( value ) * DateTimeUtils.MILLIS_PER_DAY );
                case TIME -> PolyTime.of( (int) (Long.parseLong( value ) % DateTimeUtils.MILLIS_PER_DAY) );
                case TIMESTAMP -> PolyTimestamp.of( Long.parseLong( value ) );
                case CHAR, VARCHAR, TEXT -> PolyString.of( value );
                default -> null;
            };
        } catch ( RuntimeException e ) {
            return null;
        }
    }


    /**
     * Converts a string value into a PolyValue if it is compatible with
     * the requested logical column type.
     */
    public PolyValue fromStringToCompatiblePolyValue( PolyType columnType, PolyType polyType, String value ) {
        PolyValue polyValue = fromStringToPolyValue( polyType, value );
        return isCompatible( columnType, polyValue ) ? polyValue : null;
    }


    /**
     * Converts a {@code String} value to a real comparable type.
     */
    public Comparable<?> fromStringToComparable( PolyType type, String value ) {
        if ( value == null ) {
            return null;
        }
        try {
            PolyValue polyValue = fromStringToPolyValue( type, value );
            return fromPolyValueToComparable( polyValue, type );
        } catch ( RuntimeException e ) {
            return null;
        }
    }


    /**
     * Converts a {@link PolyValue} to {@link Comparable}.
     */
    @SuppressWarnings("DataFlowIssue")
    public Comparable<?> fromPolyValueToComparable( PolyValue value, PolyType type ) {
        if ( value == null || value.isNull() ) {
            return null;
        }
        try {
            return switch ( type ) {
                case BOOLEAN -> value.isBoolean() ? value.asBoolean().value : Boolean.parseBoolean( value.toString() );
                case TINYINT, SMALLINT, INTEGER, BIGINT -> value.asNumber().longValue();
                case FLOAT, REAL -> value.asNumber().floatValue();
                case DOUBLE -> value.asNumber().doubleValue();
                case DATE -> value.isDate() ? value.asDate().getDaysSinceEpoch() : value.asNumber().longValue();
                case TIME -> value.isTime() ? value.asTime().ofDay.longValue() : value.asNumber().longValue();
                case TIMESTAMP -> value.isTimestamp() ? value.asTimestamp().millisSinceEpoch : value.asNumber().longValue();
                case CHAR, VARCHAR, TEXT -> value.isString() ? value.asString().value : value.toString();
                default -> null;
            };
        } catch ( RuntimeException e ) {
            return null;
        }
    }


    public int compareStringValues( PolyType type, String left, String right ) {
        Comparable<?> leftComparable = fromStringToComparable( type, left );
        Comparable<?> rightComparable = fromStringToComparable( type, right );
        if ( leftComparable == null || rightComparable == null ) {
            return left.compareTo( right );
        }
        return compare( leftComparable, rightComparable );
    }


    @SuppressWarnings({ "rawtypes", "unchecked" })
    public int compare( Comparable left, Comparable right ) {
        try {
            return left.compareTo( right );
        } catch ( RuntimeException e ) {
            return left.toString().compareTo( right.toString() );
        }
    }


    private boolean isCompatible( PolyType columnType, PolyValue polyValue ) {
        if ( polyValue == null ) {
            return false;
        }
        if ( columnType.getFamily() == PolyTypeFamily.NUMERIC && polyValue.isNumber() ) {
            return true;
        }
        if ( PolyType.DATETIME_TYPES.contains( columnType ) && polyValue.isTemporal() ) {
            return true;
        }
        return columnType.getFamily() == PolyTypeFamily.CHARACTER && polyValue.isString();
    }


    private Object fromStringToDate( PrimitiveType primitive, String literal ) {
        LogicalTypeAnnotation logical = primitive.getLogicalTypeAnnotation();

        if ( logical instanceof DateLogicalTypeAnnotation ) {
            LocalDate date = parseLocalDate( literal );
            return date == null ? Integer.parseInt( literal ) : (int) date.toEpochDay();
        }

        if ( logical instanceof TimeLogicalTypeAnnotation timeLogical ) {
            long millis = normalizeTimeLiteralToMillis( literal );
            return switch ( timeLogical.getUnit() ) {
                case MILLIS -> parquetTimeMillisValue( primitive, millis );
                case MICROS -> millis * 1_000L;
                case NANOS -> millis * 1_000_000L;
            };
        }

        if ( logical instanceof TimestampLogicalTypeAnnotation timestampLogical ) {
            long millis = normalizeTimestampLiteralToMillis( literal );
            return switch ( timestampLogical.getUnit() ) {
                case MILLIS -> millis;
                case MICROS -> millis * 1_000L;
                case NANOS -> millis * 1_000_000L;
            };
        }
        return null;
    }


    private Object fromPolyValueToDate( PrimitiveType primitive, PolyValue literal ) {
        LogicalTypeAnnotation logical = primitive.getLogicalTypeAnnotation();

        if ( logical instanceof DateLogicalTypeAnnotation ) {
            if ( literal.isDate() ) {
                return Math.toIntExact( literal.asDate().getDaysSinceEpoch() );
            }
            if ( literal.isNumber() ) {
                return literal.asNumber().intValue();
            }
            return fromStringToDate( primitive, literal.toString() );
        }

        if ( logical instanceof TimeLogicalTypeAnnotation timeLogical ) {
            long millis;
            if ( literal.isTime() ) {
                millis = Objects.requireNonNull( literal.asTime().ofDay ).longValue();
            } else if ( literal.isNumber() ) {
                millis = normalizeTimeToMillis( literal.asNumber().longValue() );
            } else {
                millis = normalizeTimeLiteralToMillis( literal.toString() );
            }
            return switch ( timeLogical.getUnit() ) {
                case MILLIS -> parquetTimeMillisValue( primitive, millis );
                case MICROS -> millis * 1_000L;
                case NANOS -> millis * 1_000_000L;
            };
        }

        if ( logical instanceof TimestampLogicalTypeAnnotation timestampLogical ) {
            long millis;
            if ( literal.isTimestamp() ) {
                //noinspection DataFlowIssue
                millis = literal.asTimestamp().millisSinceEpoch;
            } else if ( literal.isNumber() ) {
                millis = normalizeTimestampToMillis( literal.asNumber().longValue() );
            } else {
                millis = normalizeTimestampLiteralToMillis( literal.toString() );
            }
            return switch ( timestampLogical.getUnit() ) {
                case MILLIS -> millis;
                case MICROS -> millis * 1_000L;
                case NANOS -> millis * 1_000_000L;
            };
        }

        return null;
    }


    private Object parquetTimeMillisValue( PrimitiveType primitive, long millis ) {
        if ( primitive.getPrimitiveTypeName() == PrimitiveTypeName.INT32 ) {
            return Math.toIntExact( millis );
        }
        return millis;
    }


    /**
     * Parses an ISO local date string.
     */
    private LocalDate parseLocalDate( String value ) {
        try {
            return LocalDate.parse( value, DateTimeFormatter.ISO_LOCAL_DATE );
        } catch ( DateTimeParseException e ) {
            return null;
        }
    }


    /**
     * Parses an ISO local time string.
     */
    private LocalTime parseLocalTime( String value ) {
        try {
            return LocalTime.parse( value, DateTimeFormatter.ISO_LOCAL_TIME );
        } catch ( DateTimeParseException e ) {
            return null;
        }
    }


    /**
     * Parses time literal and normalizes it to milliseconds.
     */
    private long normalizeTimeLiteralToMillis( String literal ) {
        LocalTime localTime = parseLocalTime( literal );
        if ( localTime != null ) {
            return localTime.toNanoOfDay() / 1_000_000L;
        }
        return normalizeTimeToMillis( Long.parseLong( literal ) );
    }


    /**
     * Parses timestamp literal and normalizes it to milliseconds.
     */
    private long normalizeTimestampLiteralToMillis( String literal ) {
        LocalDateTime localDateTime = parseLocalDateTime( literal );
        if ( localDateTime != null ) {
            return localDateTime.toInstant( ZoneOffset.UTC ).toEpochMilli();
        }
        return normalizeTimestampToMillis( Long.parseLong( literal ) );
    }


    /**
     * Normalizes Parquet TIME footer statistics to milliseconds.
     */
    private long normalizeParquetTimeToMillis( PrimitiveType primitive, long value ) {
        LogicalTypeAnnotation logical = primitive.getLogicalTypeAnnotation();
        if ( logical instanceof TimeLogicalTypeAnnotation timeLogical ) {
            return switch ( timeLogical.getUnit() ) {
                case MILLIS -> value;
                case MICROS -> value / 1_000L;
                case NANOS -> value / 1_000_000L;
            };
        }
        return value;
    }


    /**
     * Normalizes Parquet TIMESTAMP footer statistics to milliseconds.
     */
    private long normalizeParquetTimestampToMillis( PrimitiveType primitive, long value ) {
        LogicalTypeAnnotation logical = primitive.getLogicalTypeAnnotation();
        if ( logical instanceof TimestampLogicalTypeAnnotation timestampLogical ) {
            return switch ( timestampLogical.getUnit() ) {
                case MILLIS -> value;
                case MICROS -> value / 1_000L;
                case NANOS -> value / 1_000_000L;
            };
        }
        return value;
    }


    /**
     * Normalizes time units to milliseconds.
     */
    private long normalizeTimeToMillis( long value ) {
        long abs = Math.abs( value );
        if ( abs > 86_400_000_000L ) {
            return value / 1_000_000L;
        }
        if ( abs > 86_400_000L ) {
            return value / 1_000L;
        }
        return value;
    }


    /**
     * Normalizes timestamp units to milliseconds.
     */
    private long normalizeTimestampToMillis( long value ) {
        long abs = Math.abs( value );
        if ( abs > 100_000_000_000_000_000L ) {
            return value / 1_000_000L;
        }
        if ( abs > 100_000_000_000_000L ) {
            return value / 1_000L;
        }
        return value;
    }


    /**
     * Converts to boolean value.
     */
    private PolyValue fromObjToPolyBoolean( Object value ) {
        if ( value instanceof Boolean b ) {
            return PolyBoolean.of( b );
        }
        return PolyBoolean.of( Boolean.parseBoolean( String.valueOf( value ) ) );
    }


    /**
     * Converts to binary value.
     */
    private PolyValue fromObjToPolyBinary( Object value ) {
        if ( value instanceof byte[] bytes ) {
            return PolyBinary.of( bytes );
        }
        if ( value instanceof Binary binary ) {
            return PolyBinary.of( binary.getBytes() );
        }
        return PolyBinary.of( String.valueOf( value ).getBytes( StandardCharsets.UTF_8 ) );
    }


    /**
     * Converts to string value.
     */
    private PolyValue fromObjToPolyString( Object value ) {
        if ( value instanceof Binary binary ) {
            return PolyString.of( binary.toStringUsingUTF8() );
        }
        if ( value instanceof byte[] bytes ) {
            return PolyString.of( new String( bytes, StandardCharsets.UTF_8 ) );
        }
        return PolyString.of( String.valueOf( value ) );
    }


    /**
     * Converts to integer value.
     */
    private PolyValue fromObjToPolyInteger( Object value ) {
        if ( value instanceof Number n ) {
            return PolyInteger.of( n );
        }
        return PolyInteger.of( Integer.parseInt( String.valueOf( value ) ) );
    }


    /**
     * Converts to long value.
     */
    private PolyValue fromObjToPolyLong( Object value ) {
        if ( value instanceof Number n ) {
            return PolyLong.of( n );
        }
        return PolyLong.of( Long.parseLong( String.valueOf( value ) ) );
    }


    /**
     * Converts to float value.
     */
    private PolyValue fromObjToPolyFloat( Object value ) {
        if ( value instanceof Number n ) {
            return PolyFloat.of( n );
        }
        return PolyFloat.of( Float.parseFloat( String.valueOf( value ) ) );
    }


    /**
     * Converts to double value.
     */
    private PolyValue fromObjToPolyDouble( Object value ) {
        if ( value instanceof Number n ) {
            return PolyDouble.of( n );
        }
        return PolyDouble.of( Double.parseDouble( String.valueOf( value ) ) );
    }


    /**
     * Converts to date value.
     */
    private PolyValue fromObjToPolyDate( Object value ) {
        if ( value instanceof Number n ) {
            long numeric = n.longValue();
            if ( Math.abs( numeric ) < 10_000_000L ) {
                return PolyDate.of( numeric * DateTimeUtils.MILLIS_PER_DAY );
            }
            return PolyDate.of( numeric );
        }
        if ( value instanceof LocalDate localDate ) {
            return PolyDate.of( localDate.toEpochDay() * DateTimeUtils.MILLIS_PER_DAY );
        }
        if ( value instanceof Date date ) {
            return PolyDate.of( date );
        }

        LocalDate localDate = parseLocalDate( String.valueOf( value ) );
        if ( localDate == null ) {
            throw new IllegalArgumentException( "Cannot parse DATE object: " + value );
        }
        return PolyDate.of( localDate.toEpochDay() * DateTimeUtils.MILLIS_PER_DAY );
    }


    /**
     * Converts to time value.
     */
    private PolyValue fromObjToPolyTime( Object value ) {
        long millis;
        if ( value instanceof Number n ) {
            millis = normalizeTimeToMillis( n.longValue() );
        } else if ( value instanceof LocalTime localTime ) {
            millis = localTime.toNanoOfDay() / 1_000_000L;
        } else {
            LocalTime localTime = parseLocalTime( String.valueOf( value ) );
            if ( localTime == null ) {
                throw new IllegalArgumentException( "Cannot parse TIME object: " + value );
            }
            millis = localTime.toNanoOfDay() / 1_000_000L;
        }
        return PolyTime.of( (int) (millis % DateTimeUtils.MILLIS_PER_DAY) );
    }


    /**
     * Converts to timestamp value.
     */
    private PolyValue fromObjToPolyTimestamp( Object value ) {
        long millis;
        if ( value instanceof Number n ) {
            millis = normalizeTimestampToMillis( n.longValue() );
        } else if ( value instanceof LocalDateTime localDateTime ) {
            millis = localDateTime.toInstant( ZoneOffset.UTC ).toEpochMilli();
        } else if ( value instanceof Date date ) {
            millis = date.getTime();
        } else {
            LocalDateTime localDateTime = parseLocalDateTime( String.valueOf( value ) );
            if ( localDateTime == null ) {
                millis = Long.parseLong( String.valueOf( value ) );
            } else {
                millis = localDateTime.toInstant( ZoneOffset.UTC ).toEpochMilli();
            }
        }
        return PolyTimestamp.of( millis );
    }


}
