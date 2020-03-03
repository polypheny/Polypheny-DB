/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db;


import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.function.Function;


public enum PolySqlType {

    BOOLEAN( Types.BOOLEAN, PolySqlTypeCode.BOOLEAN, new Class[0] ) {
        private final Class<Boolean> clazz = Boolean.class;


        @Override
        public Class<Boolean> getTypeJavaClass() {
            return this.clazz;
        }
    },

    VARBINARY( Types.VARBINARY, PolySqlTypeCode.VARBINARY, Integer.class ) {

        private final Class<byte[]> clazz = (Class<byte[]>) (new byte[0]).getClass();


        @Override
        public Class<byte[]> getTypeJavaClass() {
            return this.clazz;
        }
    },

    INTEGER( Types.INTEGER, PolySqlTypeCode.INTEGER, new Class[0] ) {
        private final Class<Integer> clazz = Integer.class;


        @Override
        public Class<Integer> getTypeJavaClass() {
            return this.clazz;
        }
    },

    BIGINT( Types.BIGINT, PolySqlTypeCode.BIGINT, new Class[0] ) {
        private final Class<Long> clazz = Long.class;


        @Override
        public Class<Long> getTypeJavaClass() {
            return this.clazz;
        }
    },

    REAL( Types.REAL, PolySqlTypeCode.REAL, new Class[0] ) {
        private final Class<Float> clazz = Float.class;


        @Override
        public Class<Float> getTypeJavaClass() {
            return this.clazz;
        }
    },

    DOUBLE( Types.DOUBLE, PolySqlTypeCode.DOUBLE, new Class[0] ) {
        private final Class<Double> clazz = Double.class;


        @Override
        public Class<Double> getTypeJavaClass() {
            return this.clazz;
        }
    },

    DECIMAL( Types.DECIMAL, PolySqlTypeCode.DECIMAL, Integer.class, Integer.class ) {
        private final Class<BigDecimal> clazz = BigDecimal.class;


        @Override
        public Class<BigDecimal> getTypeJavaClass() {
            return this.clazz;
        }
    },

    VARCHAR( Types.VARCHAR, PolySqlTypeCode.VARCHAR, Integer.class ) {
        private final Class<String> clazz = String.class;


        @Override
        public Class<String> getTypeJavaClass() {
            return this.clazz;
        }
    },

    TEXT( Types.VARCHAR, PolySqlTypeCode.TEXT, new Class[0] ) {
        private final Class<String> clazz = String.class;


        @Override
        public Class<String> getTypeJavaClass() {
            return this.clazz;
        }
    },

    DATE( Types.DATE, PolySqlTypeCode.DATE, new Class[0] ) {
        private final Class<Date> clazz = Date.class;


        @Override
        public Class<Date> getTypeJavaClass() {
            return this.clazz;
        }
    },

    TIME( Types.TIME, PolySqlTypeCode.TIME, new Class[0] ) {
        private final Class<Time> clazz = Time.class;


        @Override
        public Class<Time> getTypeJavaClass() {
            return this.clazz;
        }
    },

    TIMESTAMP( Types.TIMESTAMP, PolySqlTypeCode.TIMESTAMP, new Class[0] ) {
        private final Class<Timestamp> clazz = Timestamp.class;


        @Override
        public Class<Timestamp> getTypeJavaClass() {
            return this.clazz;
        }
    },

//
// BLOB and CLOB won't be supported in the first PolySQL version
//
//  BLOB( java.sql.PolySqlType.BLOB, java.sql.Blob.class.getName() ),
//  CLOB( java.sql.PolySqlType.CLOB, java.sql.Clob.class.getName() ),
//
    ;


    public static class PolySqlTypeCode {

        public static final int BOOLEAN = 1;
        public static final int VARBINARY = 2;
        public static final int INTEGER = 3;
        public static final int BIGINT = 4;
        public static final int REAL = 5;
        public static final int DOUBLE = 6;
        public static final int DECIMAL = 7;
        public static final int VARCHAR = 9;
        public static final int TEXT = 10;
        public static final int DATE = 11;
        public static final int TIME = 12;
        public static final int TIMESTAMP = 13;
    }


    private final int javaSqlTypesConstant;
    private final int typeCode;
    private final Class[] parameterTypes;


    public boolean isCharType() {
        return this == VARCHAR || this == TEXT;
    }


    public boolean isNumericalType() {
        return this == INTEGER || this == BIGINT || this == REAL || this == DOUBLE || this == DECIMAL;
    }


    PolySqlType( final int javaSqlTypesConstant, final int typeCode, final Class... parameterTypes ) {
        this.javaSqlTypesConstant = javaSqlTypesConstant;
        this.typeCode = typeCode;
        if ( parameterTypes == null ) {
            this.parameterTypes = new Class[0];
        } else {
            this.parameterTypes = parameterTypes;
        }
    }


    public int getJavaSqlTypesValue() {
        return this.javaSqlTypesConstant;
    }


    public Class[] getParameterTypes() {
        return parameterTypes;
    }


    public int getTypeCode() {
        return typeCode;
    }


    public abstract Class<?> getTypeJavaClass();


    public static Class<?> getTypeJavaClass( final PolySqlType polySqlType ) {
        return polySqlType.getTypeJavaClass();
    }


    public static Class<?> getTypeJavaClass( final int javaSqlTypesValue ) {
        return getPolySqlTypeFromJavaSqlType( javaSqlTypesValue ).getTypeJavaClass();
    }


    public String getTypeClassName() {
        return this.getTypeJavaClass().getName();
    }


    public static String getTypeClassName( final PolySqlType polySqlType ) {
        return polySqlType.getTypeClassName();
    }


    public static String getTypeClassName( final int javaSqlTypesValue ) {
        return getPolySqlTypeFromJavaSqlType( javaSqlTypesValue ).getTypeClassName();
    }


    public static PolySqlType getPolySqlTypeFromSting( final String str ) {
        return valueOf( str.toUpperCase() );
    }


    public static PolySqlType getByTypeCode( final int typeCode ) throws UnknownTypeException {
        switch ( typeCode ) {
            case PolySqlTypeCode.BOOLEAN:
                return BOOLEAN;
            case PolySqlTypeCode.VARBINARY:
                return VARBINARY;
            case PolySqlTypeCode.INTEGER:
                return INTEGER;
            case PolySqlTypeCode.BIGINT:
                return BIGINT;
            case PolySqlTypeCode.REAL:
                return REAL;
            case PolySqlTypeCode.DOUBLE:
                return DOUBLE;
            case PolySqlTypeCode.DECIMAL:
                return DECIMAL;
            case PolySqlTypeCode.VARCHAR:
                return VARCHAR;
            case PolySqlTypeCode.TEXT:
                return TEXT;
            case PolySqlTypeCode.DATE:
                return DATE;
            case PolySqlTypeCode.TIME:
                return TIME;
            case PolySqlTypeCode.TIMESTAMP:
                return TIMESTAMP;
            default:
                throw new UnknownTypeException( "A PolySqlType was not found for the given type code '" + typeCode + "'" );
        }
    }


    /**
     * Returns the enum constant of this type with the specified java.sql.PolySqlType constant.
     *
     * @param javaSqlTypesConstant see java.sql.Types
     * @return the enum constant with the specified java.sql.PolySqlType constant
     * @throws IllegalArgumentException if this enum type has no constant with the specified name
     * @see Types
     */
    public static PolySqlType getPolySqlTypeFromJavaSqlType( final int javaSqlTypesConstant ) {
        switch ( javaSqlTypesConstant ) {
            case Types.BIT:
            case Types.BOOLEAN:
                return BOOLEAN;

            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
                return INTEGER;

            case Types.BIGINT:
                return BIGINT;

            case Types.REAL:
                return REAL;

            case Types.FLOAT:
            case Types.DOUBLE:
                return DOUBLE;

            case Types.NUMERIC:
            case Types.DECIMAL:
                return DECIMAL;

            case Types.CHAR:
            case Types.VARCHAR:
                return VARCHAR;

            case Types.LONGVARCHAR:
                return TEXT;

            case Types.DATE:
                return DATE;

            case Types.TIME:
                return TIME;

            case Types.TIMESTAMP:
                return TIMESTAMP;

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return VARBINARY;

            case Types.NULL:
            case Types.OTHER:
            case Types.JAVA_OBJECT:
            case Types.DISTINCT:
            case Types.STRUCT:
            case Types.ARRAY:
            case Types.BLOB:
            case Types.CLOB:
            case Types.REF:
            case Types.DATALINK:
            case Types.ROWID:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.NCLOB:
            case Types.SQLXML:
            case Types.REF_CURSOR:
            case Types.TIME_WITH_TIMEZONE:
            case Types.TIMESTAMP_WITH_TIMEZONE:
            default:
                throw new IllegalArgumentException( "PolySQL has no data type defined for java.sql.PolySqlType '" + javaSqlTypesConstant + "'" );
        }
    }


    public static boolean canConvertFromTo( PolySqlType in, PolySqlType out ) {
        switch ( in ) {
            case BOOLEAN:
                switch ( out ) {
                    case BOOLEAN:
                    case INTEGER:
                    case TEXT:
                    case VARCHAR:
                        return true;
                }
                break;
            case VARBINARY:
                switch ( out ) {
                    case VARBINARY:
                    case VARCHAR:
                    case TEXT:
                        return true;
                }
                break;
            case BIGINT:
            case INTEGER:
                switch ( out ) {
                    case INTEGER:
                    case BOOLEAN:
                    case DOUBLE:
                    case REAL:
                    case BIGINT:
                    case VARCHAR:
                    case TEXT:
                        return true;
                }
                break;
            case REAL:
            case DOUBLE:
                switch ( out ) {
                    case INTEGER:
                    case DECIMAL:
                    case REAL:
                    case DOUBLE:
                    case TEXT:
                    case VARCHAR:
                        return true;
                }
                break;
            case DECIMAL:
                switch ( out ) {
                    case DECIMAL:
                    case INTEGER:
                    case BIGINT:
                    case REAL:
                    case DOUBLE:
                    case TEXT:
                    case VARCHAR:
                        return true;
                }
                break;
            case VARCHAR:
            case TEXT:
                switch ( out ) {
                    case VARCHAR:
                    case TEXT:
                    case BOOLEAN:
                    case BIGINT:
                    case INTEGER:
                    case DECIMAL:
                    case VARBINARY:
                    case DATE:
                    case TIME:
                    case TIMESTAMP:
                    case REAL:
                    case DOUBLE:
                        return true;
                }
                break;
            case DATE:
            case TIMESTAMP:
                switch ( out ) {
                    case DATE:
                    case TIME:
                    case TIMESTAMP:
                    case TEXT:
                    case VARCHAR:
                        return true;
                }
                break;
            case TIME:
                switch ( out ) {
                    case TIME:
                    case TEXT:
                    case VARCHAR:
                        return true;
                }
                break;
        }
        return false;
    }


    public static Function<Object, Object> convertFromTo( PolySqlType in, PolySqlType out ) {
//        Function<Object, Object> converter = null;

        switch ( in ) {
            case BOOLEAN:
                switch ( out ) {
                    case BOOLEAN:
                        return Function.identity();
                    case INTEGER:
                        return bool -> (((Boolean) bool) ? 1 : 0);
                    case TEXT:
                    case VARCHAR:
                        return bool -> (((Boolean) bool) ? "true" : "false");
//                        return null;
                }
                break;
            case VARBINARY:
                switch ( out ) {
                    case VARBINARY:
                        return Function.identity();
                    case VARCHAR:
                    case TEXT:
                        // TODO JS: How are VARBINARY represented in java?
                        break;
                }
                break;
            case BIGINT:
            case INTEGER:
                switch ( out ) {
                    case INTEGER:
                    case BIGINT:
                        return Function.identity();
                    case BOOLEAN:
                        return integer -> ((Integer) integer) != 0;
                    case DOUBLE:
                    case REAL:
                        return integer -> ((Integer) integer).doubleValue();
                    case VARCHAR:
                    case TEXT:
                        return integer -> ((Integer) integer).toString();
                }
                break;
            case REAL:
            case DOUBLE:
                switch ( out ) {
                    case REAL:
                    case DOUBLE:
                        return Function.identity();
                    case INTEGER:
                        return floating -> ((Double) floating).intValue();
                    case DECIMAL:
                        // TODO JS: Figure out how to convert this.
                        break;
                    case TEXT:
                    case VARCHAR:
                        return floating -> ((Double) floating).toString();
                }
                break;
            case DECIMAL:
                switch ( out ) {
                    case DECIMAL:
                        return Function.identity();
                    case INTEGER:
                    case BIGINT:
                    case REAL:
                    case DOUBLE:
                    case TEXT:
                    case VARCHAR:
                        break;
                }
                break;
            case VARCHAR:
            case TEXT:
                switch ( out ) {
                    case VARCHAR:
                    case TEXT:
                        return Function.identity();
                    case BOOLEAN:
                        return boolString -> {
                            String bString = (String) boolString;
                            if ( bString.equalsIgnoreCase( "true" ) ) return true;
                            if ( bString.equalsIgnoreCase( "false" ) ) return false;
                            throw new RuntimeException( "Unable to converter string to boolean. Given string was: " + bString );
                        };
                    case BIGINT:
                    case INTEGER:
                        return intString -> Integer.valueOf( (String) intString );
                    case REAL:
                    case DOUBLE:
                        return doubleString -> Double.valueOf( (String) doubleString );
                    case DECIMAL:
                    case VARBINARY:
                    case DATE:
                    case TIME:
                    case TIMESTAMP:
                        break;
                }
                break;
            case DATE:
            case TIMESTAMP:
                switch ( out ) {
                    case DATE:
                    case TIMESTAMP:
                        return Function.identity();
                    case TIME:
                    case TEXT:
                    case VARCHAR:
                        break;
                }
                break;
            case TIME:
                switch ( out ) {
                    case TIME:
                        return Function.identity();
                    case TEXT:
                    case VARCHAR:
                        break;
                }
                break;
        }

        throw new RuntimeException( "Unable to convert from " + in.name() + " to " + out.name() );
    }

}

