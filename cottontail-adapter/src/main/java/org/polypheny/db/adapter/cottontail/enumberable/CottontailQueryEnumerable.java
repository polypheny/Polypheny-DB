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
 */

package org.polypheny.db.adapter.cottontail.enumberable;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.function.Function1;
import org.polypheny.db.adapter.cottontail.algebra.CottontailToEnumerableConverter;
import org.polypheny.db.adapter.cottontail.util.Linq4JFixer;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.sql.language.fun.SqlArrayValueConstructor;
import org.polypheny.db.type.ArrayType;
import org.vitrivr.cottontail.client.iterators.Tuple;
import org.vitrivr.cottontail.client.iterators.TupleIterator;

@Slf4j
public class CottontailQueryEnumerable extends AbstractEnumerable<Object> {

    /**
     * The {@link TupleIterator} backing this {@link CottontailQueryEnumerable}.
     */
    private final TupleIterator tupleIterator;

    /**
     * The {@link RowTypeParser} backing this {@link CottontailQueryEnumerable}.
     */
    private final Function1<Tuple, Object[]> parser;


    public CottontailQueryEnumerable( TupleIterator iterator, Function1<Tuple, Object[]> rowParser ) {
        this.tupleIterator = iterator;
        this.parser = rowParser;
    }


    @Override
    public Enumerator<Object> enumerator() {
        return new CottontailQueryResultEnumerator();
    }


    private class CottontailQueryResultEnumerator implements Enumerator<Object> {

        /**
         * The current {@link Tuple} this {@link CottontailQueryEnumerable} is pointing to.
         */
        private Tuple tuple = null;


        @Override
        public Object current() {
            final Object[] results = CottontailQueryEnumerable.this.parser.apply( this.tuple );
            if ( results.length == 1 ) {
                return results[0];
            } else {
                return results;
            }
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
                log.warn( "Caught exception", e );
            }
        }

    }


    public static class RowTypeParser implements Function1<Tuple, Object[]> {

        private final AlgDataType rowType;
        private final List<String> physicalColumnNames;


        public RowTypeParser( AlgDataType rowType, List<String> physicalColumnNames ) {
            this.rowType = rowType;
            this.physicalColumnNames = physicalColumnNames;
        }


        @Override
        public Object[] apply( Tuple a0 ) {
            final Object[] returnValue = new Object[this.physicalColumnNames.size()];
            final List<AlgDataTypeField> fieldList = this.rowType.getFieldList();
            for ( int i = 0; i < fieldList.size(); i++ ) {
                final AlgDataType type = fieldList.get( i ).getType();
                returnValue[i] = this.parseSingleField( a0.get( i ), type );
            }

            return returnValue;
        }


        /**
         * Internal method used to parse a single value returned from accessing a {@link Tuple}.
         *
         * @param data The data to convert.
         * @param type The {@link AlgDataType} expected by Polypheny-DB
         * @return Converted value as {@link Object}
         */
        private Object parseSingleField( Object data, AlgDataType type ) {
            switch ( type.getPolyType() ) {
                case BOOLEAN:
                case INTEGER:
                case BIGINT:
                case FLOAT:
                case REAL:
                case DOUBLE:
                case CHAR:
                case VARCHAR:
                case NULL:
                    return data; /* Pass through, no conversion needed. */
                case TINYINT:
                    return Linq4JFixer.getTinyIntData( data );
                case SMALLINT:
                    return Linq4JFixer.getSmallIntData( data );
                case JSON:
                    return Linq4JFixer.getStringData( data );
                case DECIMAL:
                    return Linq4JFixer.getDecimalData( data );
                case DATE:
                    return Linq4JFixer.getDateData( data );
                case TIME:
                    return Linq4JFixer.getTimeData( data );
                case TIMESTAMP:
                    return Linq4JFixer.getTimestampData( data );
                case BINARY:
                case VARBINARY:
                    return Linq4JFixer.getBinaryData( data );
                case ARRAY:
                    ArrayType arrayType = (ArrayType) type;
                    if ( arrayType.getDimension() == 1 && CottontailToEnumerableConverter.SUPPORTED_ARRAY_COMPONENT_TYPES.contains( arrayType.getComponentType().getPolyType() ) ) {
                        switch ( arrayType.getComponentType().getPolyType() ) {
                            case INTEGER:
                                return Linq4JFixer.getIntVector( data );
                            case BIGINT:
                                return Linq4JFixer.getLongVector( data );
                            case DOUBLE:
                                return Linq4JFixer.getDoubleVector( data );
                            case BOOLEAN:
                                return Linq4JFixer.getBoolVector( data );
                            case FLOAT:
                            case REAL:
                                return Linq4JFixer.getFloatVector( data );
                            default:
                                throw new RuntimeException( "Impossible to reach statement." );
                        }
                    } else {
                        SqlArrayValueConstructor.reparse( arrayType.getComponentType().getPolyType(), arrayType.getDimension(), (String) data );
                    }
            }
            throw new AssertionError( "Not yet supported type: " + type.getPolyType() );
        }

    }

}
