/*
 * Copyright 2019-2021 The Polypheny Project
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


import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;


public class PolyTypeConversionUtil {

    public static Object convertValue( Object value, PolyType inType, PolyType outType ) {

        switch ( inType ) {
            case BOOLEAN:
                return fromBoolean( value, outType );
            case TINYINT:
                return fromTinyint( value, outType );
            case SMALLINT:
                return fromSmallint( value, outType );
            case INTEGER:
                return fromInt( value, outType );
            case BIGINT:
                return fromBigint( value, outType );
            case DECIMAL:
                return fromDecimal( value, outType );
            case REAL:
                return fromReal( value, outType );
            case DOUBLE:
                return fromDouble( value, outType );
            case DATE:
                return fromDate( value, outType );
            case TIME:
                return fromTime( value, outType );
            case TIMESTAMP:
                return fromTimestamp( value, outType );
            case VARCHAR:
                return fromVarchar( value, outType );
            default:
                throw new IllegalArgumentException( "Cannot convert from type: " + inType );
        }
    }


    static Object fromBoolean( Object value, PolyType outType ) {

        if ( value instanceof List ) {
            List valueList = (List) value;
            List returnList = new ArrayList( valueList.size() );
            for ( Object inner : valueList ) {
                returnList.add( fromBoolean( inner, outType ) );
            }

            return returnList;
        }

        Boolean bool = (Boolean) value;
        switch ( outType ) {
            case BOOLEAN:
                return value;
            case TINYINT:
                return bool ? (byte) 1 : (byte) 0;
            case SMALLINT:
                return bool ? (short) 1 : (short) 0;
            case INTEGER:
                return bool ? 1 : 0;
            case BIGINT:
                return bool ? 1L : 0L;
            case DECIMAL:
                return bool ? BigDecimal.valueOf( 1L ) : BigDecimal.valueOf( 0L );
            case REAL:
                return bool ? 1.0F : 0.0F;
            case DOUBLE:
                return bool ? 1.0 : 0.0;
            case VARCHAR:
                return bool ? "true" : "false";
            default:
                throw new IllegalArgumentException( "Cannot convert boolean to type: " + outType );
        }
    }


    static Object fromTinyint( Object value, PolyType outType ) {
        if ( value instanceof List ) {
            List valueList = (List) value;
            List returnList = new ArrayList( valueList.size() );
            for ( Object inner : valueList ) {
                returnList.add( fromTinyint( inner, outType ) );
            }

            return returnList;
        }

        Byte byteValue = (Byte) value;
        switch ( outType ) {
            case TINYINT:
                return value;
            case SMALLINT:
                return (short) byteValue;
            case INTEGER:
                return (int) byteValue;
            case BIGINT:
                return (long) byteValue;
            case DECIMAL:
                return BigDecimal.valueOf( (long) byteValue );
            case REAL:
                return (float) byteValue;
            case DOUBLE:
                return (double) byteValue;
            case VARCHAR:
                return String.valueOf( byteValue );
            default:
                throw new IllegalArgumentException( "Cannot convert tinyint to type: " + outType );
        }
    }


    static Object fromSmallint( Object value, PolyType outType ) {
        if ( value instanceof List ) {
            List valueList = (List) value;
            List returnList = new ArrayList( valueList.size() );
            for ( Object inner : valueList ) {
                returnList.add( fromSmallint( inner, outType ) );
            }

            return returnList;
        }

        Short shortValue = (Short) value;
        switch ( outType ) {
            case SMALLINT:
                return value;
            case TINYINT:
                return Integer.valueOf( shortValue ).byteValue();
            case INTEGER:
                return (int) shortValue;
            case BIGINT:
                return (long) shortValue;
            case DECIMAL:
                return BigDecimal.valueOf( shortValue );
            case REAL:
                return (float) shortValue;
            case DOUBLE:
                return (double) shortValue;
            // TODO FINISH
            default:
                throw new IllegalArgumentException( "Cannot convert smallint to type: " + outType );
        }
    }


    static Object fromInt( Object value, PolyType outType ) {
        if ( value instanceof List ) {
            List valueList = (List) value;
            List returnList = new ArrayList( valueList.size() );
            for ( Object inner : valueList ) {
                returnList.add( fromInt( inner, outType ) );
            }

            return returnList;
        }

        Integer intValue = (Integer) value;
        switch ( outType ) {
            case INTEGER:
                return value;
            case TINYINT:
                return Integer.valueOf( intValue ).byteValue();
            case SMALLINT:
                return Integer.valueOf( intValue ).shortValue();
            case BIGINT:
                return (long) intValue;
            case DECIMAL:
                return BigDecimal.valueOf( intValue );
            case REAL:
                return (float) intValue;
            case DOUBLE:
                return (double) intValue;
            // TODO FINISH
            default:
                throw new IllegalArgumentException( "Cannot convert int to type: " + outType );
        }
    }


    static Object fromBigint( Object value, PolyType outType ) {
        if ( value instanceof List ) {
            List valueList = (List) value;
            List returnList = new ArrayList( valueList.size() );
            for ( Object inner : valueList ) {
                returnList.add( fromBigint( inner, outType ) );
            }

            return returnList;
        }

        Long bigintValue = (Long) value;
        switch ( outType ) {
            case BIGINT:
                return value;
            case TINYINT:
                return bigintValue.byteValue();
            case SMALLINT:
                return bigintValue.shortValue();
            case INTEGER:
                return bigintValue.intValue();
            case DECIMAL:
                return BigDecimal.valueOf( bigintValue );
            case REAL:
                return (float) bigintValue;
            case DOUBLE:
                return (double) bigintValue;
            // TODO FINISH
            default:
                throw new IllegalArgumentException( "Cannot convert bigint to type: " + outType );
        }
    }


    static Object fromDecimal( Object value, PolyType outType ) {
        if ( value instanceof List ) {
            List valueList = (List) value;
            List returnList = new ArrayList( valueList.size() );
            for ( Object inner : valueList ) {
                returnList.add( fromDecimal( inner, outType ) );
            }

            return returnList;
        }

        BigDecimal decimalValue = (BigDecimal) value;
        switch ( outType ) {
            case DECIMAL:
                return value;
            case REAL:
                return decimalValue.floatValue();
            case DOUBLE:
                return decimalValue.doubleValue();
            default:
                throw new IllegalArgumentException( "Cannot convert decimal to type: " + outType );
        }
    }


    static Object fromReal( Object value, PolyType outType ) {
        if ( value instanceof List ) {
            List valueList = (List) value;
            List returnList = new ArrayList( valueList.size() );
            for ( Object inner : valueList ) {
                returnList.add( fromReal( inner, outType ) );
            }

            return returnList;
        }

        Float realValue = (Float) value;
        switch ( outType ) {
            case REAL:
                return value;
            case DOUBLE:
                return Double.parseDouble( realValue.toString() );
            case DECIMAL:
                return BigDecimal.valueOf( realValue );
            default:
                throw new IllegalArgumentException( "Cannot convert real to type: " + outType );
        }
    }


    static Object fromDouble( Object value, PolyType outType ) {
        if ( value instanceof List ) {
            List valueList = (List) value;
            List returnList = new ArrayList( valueList.size() );
            for ( Object inner : valueList ) {
                returnList.add( fromDouble( inner, outType ) );
            }

            return returnList;
        }

        Double doubleValue = (Double) value;
        switch ( outType ) {
            case DOUBLE:
                return value;
            case REAL:
                return Float.parseFloat( doubleValue.toString() );
            case DECIMAL:
                return BigDecimal.valueOf( doubleValue );
            default:
                throw new IllegalArgumentException( "Cannot convert double to type: " + outType );
        }
    }


    static Object fromDate( Object value, PolyType outType ) {
        if ( value instanceof List ) {
            List valueList = (List) value;
            List returnList = new ArrayList( valueList.size() );
            for ( Object inner : valueList ) {
                returnList.add( fromDate( inner, outType ) );
            }

            return returnList;
        }

        switch ( outType ) {
            case DATE:
                return value;
            default:
                throw new IllegalArgumentException( "Cannot convert date to type: " + outType );
        }
    }


    static Object fromTime( Object value, PolyType outType ) {
        if ( value instanceof List ) {
            List valueList = (List) value;
            List returnList = new ArrayList( valueList.size() );
            for ( Object inner : valueList ) {
                returnList.add( fromTime( inner, outType ) );
            }

            return returnList;
        }

        switch ( outType ) {
            case TIME:
                return value;
            default:
                throw new IllegalArgumentException( "Cannot convert time to type: " + outType );
        }
    }


    static Object fromTimestamp( Object value, PolyType outType ) {
        if ( value instanceof List ) {
            List valueList = (List) value;
            List returnList = new ArrayList( valueList.size() );
            for ( Object inner : valueList ) {
                returnList.add( fromTimestamp( inner, outType ) );
            }

            return returnList;
        }

        switch ( outType ) {
            case TIMESTAMP:
                return value;
            default:
                throw new IllegalArgumentException( "Cannot convert timestamp to type: " + outType );
        }
    }


    static Object fromVarchar( Object value, PolyType outType ) {
        if ( value instanceof List ) {
            List valueList = (List) value;
            List returnList = new ArrayList( valueList.size() );
            for ( Object inner : valueList ) {
                returnList.add( fromVarchar( inner, outType ) );
            }

            return returnList;
        }

        String varcharValue = (String) value;
        switch ( outType ) {
            case VARCHAR:
                return value;
            case BOOLEAN:
                return Boolean.parseBoolean( varcharValue );
            case TINYINT:
                return Byte.parseByte( varcharValue );
            case SMALLINT:
                return Short.parseShort( varcharValue );
            case INTEGER:
                return Integer.parseInt( varcharValue );
            case BIGINT:
                return Long.parseLong( varcharValue );
            case REAL:
                return Float.parseFloat( varcharValue );
            case DOUBLE:
                return Double.parseDouble( varcharValue );
            default:
                throw new IllegalArgumentException( "Cannot convert varchar to type: " + outType );
        }
    }

}
