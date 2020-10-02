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

package org.polypheny.db.adapter.cottontail.enumberable;


import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc.Data;
import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc.QueryResponseMessage;
import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc.Tuple;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.function.Function1;
import org.polypheny.db.adapter.cottontail.CottontailToEnumerableConverter;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.sql.fun.SqlArrayValueConstructor;
import org.polypheny.db.type.ArrayType;


public class CottontailQueryEnumerable<T> extends AbstractEnumerable<T> {


    private Iterator<QueryResponseMessage> queryIterator;
    private Function1<Map<String, Data>, T> rowParser;


    public CottontailQueryEnumerable( Iterator<QueryResponseMessage> queryIterator, Function1<Map<String, Data>, T> rowParser ) {
        this.queryIterator = queryIterator;
        this.rowParser = rowParser;
    }


    @Override
    public Enumerator<T> enumerator() {
        return new CottontailQueryResultEnumerator<>( this.queryIterator, this.rowParser );
    }


    private static class CottontailQueryResultEnumerator<T> implements Enumerator<T> {

        private final Function1<Map<String, Data>, T> rowParser;
        private Iterator<QueryResponseMessage> queryIterator;

        private T current;
        private QueryResponseMessage currentQueryResponsePage;
        private Iterator<Tuple> currentResultIterator;

        CottontailQueryResultEnumerator( Iterator<QueryResponseMessage> queryIterator, Function1<Map<String, Data>, T> rowParser ) {
            this.queryIterator = queryIterator;
            this.rowParser = rowParser;
        }

        @Override
        public T current() {
            return this.current;
        }


        @Override
        public boolean moveNext() {
            // If the current result iterator doesn't have a next element but we have more response pages.
            if ( this.currentResultIterator == null || !this.currentResultIterator.hasNext() ) {
                if ( this.queryIterator.hasNext() ) {
                    this.currentQueryResponsePage = this.queryIterator.next();
                    this.currentResultIterator = this.currentQueryResponsePage.getResultsList().iterator();
                } else {
                    return false;
                }
            }

            // Update the current result
            if ( this.currentResultIterator.hasNext() ) {
                this.current = this.rowParser.apply( this.currentResultIterator.next().getDataMap() );
                return true;
            }

            return false;
        }


        @Override
        public void reset() {
            // TODO js(ct): do we need to do something here?
//            throw new RuntimeException( "Cottontail does not yet support reset" );
        }


        @Override
        public void close() {
            // TODO js(ct): do we need to do something here?
        }
    }

    public static class RowTypeParser implements Function1<Map<String, Data>, Object[]> {

        private final RelDataType rowType;
        private final List<String> physicalColumnNames;

        public RowTypeParser( RelDataType rowType, List<String> physicalColumnNames ) {
            this.rowType = rowType;
            this.physicalColumnNames = physicalColumnNames;
        }

        @Override
        public Object[] apply( Map<String, Data> a0 ) {
            Object[] returnValue = new Object[this.physicalColumnNames.size()];
//            List<Object> returnValue = new ArrayList<>( this.physicalColumnNames.size() );

            List<RelDataTypeField> fieldList = this.rowType.getFieldList();
            for ( int i = 0; i < fieldList.size(); i++ ) {
                RelDataType type = fieldList.get( i ).getType();
                String columnName = this.physicalColumnNames.get( i );
                returnValue[i] = this.parseSingleField( a0.get( columnName ), type );
            }

            return returnValue;
        }


        private Object parseSingleField( Data data, RelDataType type ) {
            switch ( type.getPolyType() ) {
                case BOOLEAN:
                    return data.getBooleanData();
                case INTEGER:
                    return data.getIntData();
                case BIGINT:
                    return data.getLongData();
                case FLOAT:
                case REAL:
                    return data.getFloatData();
                case DOUBLE:
                    return data.getDoubleData();
                case CHAR:
                case VARCHAR:
                    return data.getStringData();
                case NULL:
                    return null;
                case DECIMAL:
                    return new BigDecimal( data.getStringData() );
                case BINARY:
                case VARBINARY:
                    return ByteString.parseBase64( data.getStringData() );
                case ARRAY:
                    ArrayType arrayType = (ArrayType) type;
                    if ( arrayType.getDimension() == 1 && CottontailToEnumerableConverter.SUPPORTED_ARRAY_COMPONENT_TYPES.contains( arrayType.getComponentType().getPolyType() ) ) {
                        switch ( arrayType.getComponentType().getPolyType() ) {
                            case INTEGER:
                                return data.getVectorData().getIntVector().getVectorList();
                            case BIGINT:
                                return data.getVectorData().getLongVector().getVectorList();
                            case DOUBLE:
                                return data.getVectorData().getDoubleVector().getVectorList();
                            case BOOLEAN:
                                return data.getVectorData().getBoolVector().getVectorList();
                            case FLOAT:
                            case REAL:
                                return data.getVectorData().getFloatVector().getVectorList();
                            default:
                                throw new RuntimeException( "Impossible to reach statement." );
                        }
                    } else {
                        SqlArrayValueConstructor.reparse( arrayType.getComponentType().getPolyType(), arrayType.getDimension(), data.getStringData() );
                    }
            }
            throw new AssertionError( "Not yet supported type: " + type.getPolyType() );
        }
    }
}
