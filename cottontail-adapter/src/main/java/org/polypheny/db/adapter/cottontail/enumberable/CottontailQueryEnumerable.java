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

package org.polypheny.db.adapter.cottontail.enumberable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.function.Function1;
import org.polypheny.db.adapter.cottontail.CottontailToEnumerableConverter;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.sql.fun.SqlArrayValueConstructor;
import org.polypheny.db.type.ArrayType;
import org.vitrivr.cottontail.client.iterators.Tuple;
import org.vitrivr.cottontail.client.iterators.TupleIterator;


public class CottontailQueryEnumerable<T> extends AbstractEnumerable<T> {

    /**
     * The {@link TupleIterator} backing this {@link CottontailQueryEnumerable}.
     */
    private final TupleIterator tupleIterator;

    /**
     * The {@link RowTypeParser} backing this {@link CottontailQueryEnumerable}.
     */
    private final Function1<Tuple, T> parser;


    public CottontailQueryEnumerable( TupleIterator iterator, Function1<Tuple, T> rowParser ) {
        this.tupleIterator = iterator;
        this.parser = rowParser;
    }


    @Override
    public Enumerator<T> enumerator() {
        return new CottontailQueryResultEnumerator<>();
    }


    private class CottontailQueryResultEnumerator<T> implements Enumerator<T> {

        /**
         * The current {@link Tuple} this {@link CottontailQueryEnumerable} is pointing to.
         */
        private Tuple tuple = null;


        @Override
        public T current() {
            return ((T) CottontailQueryEnumerable.this.parser.apply( this.tuple ));
        }


        @Override
        public boolean moveNext() {
            if ( CottontailQueryEnumerable.this.tupleIterator.hasNext() ) {
                this.tuple = CottontailQueryEnumerable.this.tupleIterator.next();
                return true;
            } else {
                return false;
            }
        }


        @Override
        public void reset() {
            // TODO js(ct): do we need to do something here?
        }


        @Override
        public void close() {
            try {
                CottontailQueryEnumerable.this.tupleIterator.close();
            } catch ( Exception e ) {
                e.printStackTrace();
            }
        }


        /**
         * Internal conversion method that mainly converts arrays to lists.
         *
         * @param object The object to convert.
         * @return Converted object.
         */
        private Object convert( Object object ) {
            if ( object instanceof double[] ) {
                final List<Double> list = new ArrayList<>( ((double[]) object).length );
                for ( double v : ((double[]) object) ) {
                    list.add( v );
                }
                return list;
            } else if ( object instanceof float[] ) {
                final List<Float> list = new ArrayList<>( ((float[]) object).length );
                for ( float v : ((float[]) object) ) {
                    list.add( v );
                }
                return list;
            } else if ( object instanceof long[] ) {
                final List<Long> list = new ArrayList<>( ((long[]) object).length );
                for ( long v : ((long[]) object) ) {
                    list.add( v );
                }
                return list;
            } else if ( object instanceof int[] ) {
                final List<Integer> list = new ArrayList<>( ((int[]) object).length );
                for ( int v : ((int[]) object) ) {
                    list.add( v );
                }
                return list;
            } else if ( object instanceof boolean[] ) {
                final List<Boolean> list = new ArrayList<>( ((boolean[]) object).length );
                for ( boolean v : ((boolean[]) object) ) {
                    list.add( v );
                }
                return list;
            } else {
                return object;
            }
        }

    }


    public static class RowTypeParser implements Function1<Tuple, Object[]> {

        private final RelDataType rowType;
        private final List<String> physicalColumnNames;


        public RowTypeParser( RelDataType rowType, List<String> physicalColumnNames ) {
            this.rowType = rowType;
            this.physicalColumnNames = physicalColumnNames;
        }


        @Override
        public Object[] apply( Tuple a0 ) {
            final Object[] returnValue = new Object[this.physicalColumnNames.size()];
            final List<RelDataTypeField> fieldList = this.rowType.getFieldList();
            for ( int i = 0; i < fieldList.size(); i++ ) {
                final RelDataType type = fieldList.get( i ).getType();
                final String columnName = this.physicalColumnNames.get( i );
                returnValue[i] = this.parseSingleField( a0.get( columnName ), type );
            }

            return returnValue;
        }


        private Object parseSingleField( Object data, RelDataType type ) {
            switch ( type.getPolyType() ) {
                case BOOLEAN:
                case INTEGER:
                case BIGINT:
                case FLOAT:
                case REAL:
                case DOUBLE:
                case CHAR:
                case VARCHAR:
                case JSON:
                    return data;
                case NULL:
                    return null;
                case DECIMAL:
                    return new BigDecimal( (String) data );
                case BINARY:
                case VARBINARY:
                    return ByteString.parseBase64( (String) data );
                case ARRAY:
                    ArrayType arrayType = (ArrayType) type;
                    if ( arrayType.getDimension() == 1 && CottontailToEnumerableConverter.SUPPORTED_ARRAY_COMPONENT_TYPES.contains( arrayType.getComponentType().getPolyType() ) ) {
                        final List<Object> list = new ArrayList<>( (int) arrayType.getCardinality() );
                        switch ( arrayType.getComponentType().getPolyType() ) {
                            case INTEGER:
                                for ( long v : ((int[]) data) ) {
                                    list.add( v );
                                }
                                break;
                            case BIGINT:
                                for ( long v : ((long[]) data) ) {
                                    list.add( v );
                                }
                                break;
                            case DOUBLE:
                                for ( double v : ((double[]) data) ) {
                                    list.add( v );
                                }
                                break;
                            case BOOLEAN:
                                for ( boolean v : ((boolean[]) data) ) {
                                    list.add( v );
                                }
                                break;
                            case FLOAT:
                            case REAL:
                                for ( float v : ((float[]) data) ) {
                                    list.add( v );
                                }
                                break;
                            default:
                                throw new RuntimeException( "Impossible to reach statement." );
                        }
                        return list;
                    } else {
                        SqlArrayValueConstructor.reparse( arrayType.getComponentType().getPolyType(), arrayType.getDimension(), (String) data );
                    }
            }
            throw new AssertionError( "Not yet supported type: " + type.getPolyType() );
        }

    }

}
