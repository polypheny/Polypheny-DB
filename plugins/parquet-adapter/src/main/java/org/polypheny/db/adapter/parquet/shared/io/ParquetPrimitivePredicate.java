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

package org.polypheny.db.adapter.parquet.shared.io;

import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.schema.ParquetTypeConverter;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.type.entity.PolyValue;


/**
 * Predicate compiled against a primitive Parquet projection. The idea is to have a predicate that operates directly on parquet values instead of PolyValue.
 * In addition, predicates read value directly from ColumnReader.
 */
public interface ParquetPrimitivePredicate {

    /**
     * Creates a fast reading filter that uses {@link ColumnReader} directly to read the values. Multiple filters are combined with AND predicate.
     *
     * @param projectionSchema a projected schema.
     * @param filters a list of filters to compile.
     * @return a compiled predicate the consists of per column type predicate(s).
     */
    static ParquetPrimitivePredicate compile( MessageType projectionSchema, List<ParquetAdapterFilter<PolyValue>> filters ) {
        if ( filters == null || filters.isEmpty() ) {
            return AlwaysTruePredicate.INSTANCE;
        }

        ColumnDescriptor[] descriptors = projectionSchema.getColumns().toArray( ColumnDescriptor[]::new );
        List<ParquetPrimitivePredicate> predicates = new ArrayList<>( filters.size() );
        for ( ParquetAdapterFilter<PolyValue> filter : filters ) {
            ParquetPrimitivePredicate predicate = compile( descriptors, filter );
            if ( predicate == null ) {
                return null;
            }
            predicates.add( predicate );
        }
        return predicates.size() == 1 ? predicates.get( 0 ) : new AndPredicate( predicates );
    }


    /**
     * Compiles a single filter.
     *
     * @param descriptors column descriptors of the projected schema.
     * @param filter a filter to compile.
     * @return a compiled predicate.
     */
    private static ParquetPrimitivePredicate compile( ColumnDescriptor[] descriptors, ParquetAdapterFilter<PolyValue> filter ) {
        if ( filter.isLogical() ) {
            List<ParquetPrimitivePredicate> operands = new ArrayList<>( filter.operands().size() );
            for ( ParquetAdapterFilter<PolyValue> operand : filter.operands() ) {
                ParquetPrimitivePredicate predicate = compile( descriptors, operand );
                if ( predicate == null ) {
                    return null;
                }
                operands.add( predicate );
            }
            return filter.operator() == Kind.AND ? new AndPredicate( operands ) : null;
        }

        int index = filter.columnIndex();
        if ( index < 0 || index >= descriptors.length ) {
            return null;
        }

        ColumnDescriptor descriptor = descriptors[index];
        Kind operator = filter.operator();
        if ( operator == Kind.IS_NULL || operator == Kind.IS_NOT_NULL ) {
            return new NullPredicate( index, descriptor.getMaxDefinitionLevel(), descriptor.getPrimitiveType(), operator == Kind.IS_NOT_NULL );
        }
        if ( !isComparison( operator ) ) {
            return null;
        }
        if ( filter.value() == null || filter.value().isNull() ) {
            return null;
        }

        Object expected = new ParquetTypeConverter().fromPolyValueToParquetObj( descriptor.getPrimitiveType(), filter.value() );
        if ( expected == null ) {
            return null;
        }
        return switch ( descriptor.getPrimitiveType().getPrimitiveTypeName() ) {
            case BOOLEAN -> expected instanceof Boolean value ? new BooleanPredicate( index, descriptor.getMaxDefinitionLevel(), operator, value ) : null;
            case INT32 -> expected instanceof Integer value ? new IntPredicate( index, descriptor.getMaxDefinitionLevel(), operator, value ) : null;
            case INT64 -> expected instanceof Long value ? new LongPredicate( index, descriptor.getMaxDefinitionLevel(), operator, value ) : null;
            case FLOAT -> expected instanceof Float value ? new FloatPredicate( index, descriptor.getMaxDefinitionLevel(), operator, value ) : null;
            case DOUBLE -> expected instanceof Double value ? new DoublePredicate( index, descriptor.getMaxDefinitionLevel(), operator, value ) : null;
            case BINARY, FIXED_LEN_BYTE_ARRAY, INT96 -> expected instanceof Binary value ? new BinaryPredicate( index, descriptor.getMaxDefinitionLevel(), operator, descriptor.getPrimitiveType(), value ) : null;
        };
    }


    /**
     * Checks if the operator is a comparison operator.
     *
     * @param operator an operator to check.
     * @return true if the provided operator is a comparison operator and false otherwise.
     */
    private static boolean isComparison( Kind operator ) {
        return operator == Kind.EQUALS
                || operator == Kind.NOT_EQUALS
                || operator == Kind.GREATER_THAN
                || operator == Kind.GREATER_THAN_OR_EQUAL
                || operator == Kind.LESS_THAN
                || operator == Kind.LESS_THAN_OR_EQUAL;
    }


    /**
     * Checks if the field is presented in the reader and can be read.
     *
     * @param reader a reader.
     * @param maxDefinitionLevel a definition level.
     * @return true if the field is presented in the reader and its value can be accessed and false otherwise.
     */
    static boolean isPresent( ColumnReader reader, int maxDefinitionLevel ) {
        return maxDefinitionLevel == 0 || reader.getCurrentDefinitionLevel() == maxDefinitionLevel;
    }


    /**
     * Compares the result to 0 according to the operator.
     *
     * @param result the result to compare.
     * @param operator the operator.
     * @return true if comparison is successful and false otherwise.
     */
    static boolean compare( int result, Kind operator ) {
        return switch ( operator ) {
            case EQUALS -> result == 0;
            case NOT_EQUALS -> result != 0;
            case GREATER_THAN -> result > 0;
            case GREATER_THAN_OR_EQUAL -> result >= 0;
            case LESS_THAN -> result < 0;
            case LESS_THAN_OR_EQUAL -> result <= 0;
            default -> true;
        };
    }


    /**
     * Reads a value from the reader according to its type.
     *
     * @param reader a reader to read the value from.
     * @param type a field type.
     * @return the read value.
     */
    static Object readValue( ColumnReader reader, PrimitiveType type ) {
        return switch ( type.getPrimitiveTypeName() ) {
            case BOOLEAN -> reader.getBoolean();
            case INT32 -> reader.getInteger();
            case INT64 -> reader.getLong();
            case FLOAT -> reader.getFloat();
            case DOUBLE -> reader.getDouble();
            case BINARY, FIXED_LEN_BYTE_ARRAY, INT96 -> reader.getBinary().copy();
        };
    }


    boolean matches( ColumnReader[] readers, boolean[] consumed, Object[] values );


    enum AlwaysTruePredicate implements ParquetPrimitivePredicate {
        INSTANCE;


        @Override
        public boolean matches( ColumnReader[] readers, boolean[] consumed, Object[] values ) {
            return true;
        }
    }


    record AndPredicate( List<ParquetPrimitivePredicate> operands ) implements ParquetPrimitivePredicate {

        @Override
        public boolean matches( ColumnReader[] readers, boolean[] consumed, Object[] values ) {
            for ( ParquetPrimitivePredicate operand : operands ) {
                if ( !operand.matches( readers, consumed, values ) ) {
                    return false;
                }
            }
            return true;
        }

    }


    record NullPredicate( int index, int maxDefinitionLevel, PrimitiveType type, boolean expectedPresent ) implements ParquetPrimitivePredicate {

        @Override
        public boolean matches( ColumnReader[] readers, boolean[] consumed, Object[] values ) {
            if ( consumed[index] ) {
                boolean present = values[index] != null;
                return present == expectedPresent;
            }
            consumed[index] = true;
            ColumnReader reader = readers[index];
            boolean present = isPresent( reader, maxDefinitionLevel );
            values[index] = present ? readValue( reader, type ) : null;
            reader.consume();
            return present == expectedPresent;
        }

    }


    record BooleanPredicate( int index, int maxDefinitionLevel, Kind operator, boolean expected ) implements ParquetPrimitivePredicate {

        @Override
        public boolean matches( ColumnReader[] readers, boolean[] consumed, Object[] values ) {
            if ( consumed[index] ) {
                Boolean actual = (Boolean) values[index];
                return actual != null && compare( Boolean.compare( actual, expected ), operator );
            }
            consumed[index] = true;
            ColumnReader reader = readers[index];
            boolean present = isPresent( reader, maxDefinitionLevel );
            boolean actual = present && reader.getBoolean();
            values[index] = present ? actual : null;
            boolean result = present && compare( Boolean.compare( actual, expected ), operator );
            reader.consume();
            return result;
        }

    }


    record IntPredicate( int index, int maxDefinitionLevel, Kind operator, int expected ) implements ParquetPrimitivePredicate {

        @Override
        public boolean matches( ColumnReader[] readers, boolean[] consumed, Object[] values ) {
            if ( consumed[index] ) {
                Integer actual = (Integer) values[index];
                return actual != null && compare( Integer.compare( actual, expected ), operator );
            }
            consumed[index] = true;
            ColumnReader reader = readers[index];
            boolean present = isPresent( reader, maxDefinitionLevel );
            int actual = present ? reader.getInteger() : 0;
            values[index] = present ? actual : null;
            boolean result = present && compare( Integer.compare( actual, expected ), operator );
            reader.consume();
            return result;
        }

    }


    record LongPredicate( int index, int maxDefinitionLevel, Kind operator, long expected ) implements ParquetPrimitivePredicate {

        @Override
        public boolean matches( ColumnReader[] readers, boolean[] consumed, Object[] values ) {
            if ( consumed[index] ) {
                Long actual = (Long) values[index];
                return actual != null && compare( Long.compare( actual, expected ), operator );
            }
            consumed[index] = true;
            ColumnReader reader = readers[index];
            boolean present = isPresent( reader, maxDefinitionLevel );
            long actual = present ? reader.getLong() : 0;
            values[index] = present ? actual : null;
            boolean result = present && compare( Long.compare( actual, expected ), operator );
            reader.consume();
            return result;
        }

    }


    record FloatPredicate( int index, int maxDefinitionLevel, Kind operator, float expected ) implements ParquetPrimitivePredicate {

        @Override
        public boolean matches( ColumnReader[] readers, boolean[] consumed, Object[] values ) {
            if ( consumed[index] ) {
                Float actual = (Float) values[index];
                return actual != null && compare( Float.compare( actual, expected ), operator );
            }
            consumed[index] = true;
            ColumnReader reader = readers[index];
            boolean present = isPresent( reader, maxDefinitionLevel );
            float actual = present ? reader.getFloat() : 0;
            values[index] = present ? actual : null;
            boolean result = present && compare( Float.compare( actual, expected ), operator );
            reader.consume();
            return result;
        }

    }


    record DoublePredicate( int index, int maxDefinitionLevel, Kind operator, double expected ) implements ParquetPrimitivePredicate {

        @Override
        public boolean matches( ColumnReader[] readers, boolean[] consumed, Object[] values ) {
            if ( consumed[index] ) {
                Double actual = (Double) values[index];
                return actual != null && compare( Double.compare( actual, expected ), operator );
            }
            consumed[index] = true;
            ColumnReader reader = readers[index];
            boolean present = isPresent( reader, maxDefinitionLevel );
            double actual = present ? reader.getDouble() : 0;
            values[index] = present ? actual : null;
            boolean result = present && compare( Double.compare( actual, expected ), operator );
            reader.consume();
            return result;
        }

    }


    record BinaryPredicate( int index, int maxDefinitionLevel, Kind operator, PrimitiveType type, Binary expected ) implements ParquetPrimitivePredicate {

        @Override
        public boolean matches( ColumnReader[] readers, boolean[] consumed, Object[] values ) {
            if ( consumed[index] ) {
                Binary actual = (Binary) values[index];
                return actual != null && compare( type.comparator().compare( actual, expected ), operator );
            }
            consumed[index] = true;
            ColumnReader reader = readers[index];
            boolean present = isPresent( reader, maxDefinitionLevel );
            Binary actual = present ? reader.getBinary() : null;
            values[index] = actual == null ? null : actual.copy();
            boolean result = present && compare( type.comparator().compare( actual, expected ), operator );
            reader.consume();
            return result;
        }

    }

}
