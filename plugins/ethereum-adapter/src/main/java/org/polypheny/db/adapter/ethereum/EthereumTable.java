/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.adapter.ethereum;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.FilterableTable;
import org.polypheny.db.schema.impl.AbstractTable;
import org.polypheny.db.util.Pair;
import org.web3j.abi.datatypes.Event;

public class EthereumTable extends AbstractTable implements FilterableTable {

    protected final String clientUrl;
    protected final AlgProtoDataType protoRowType;
    protected final int[] fields;
    protected final EthereumDataSource ethereumDataSource;
    protected final EthereumMapper mapper;
    protected List<EthereumFieldType> fieldTypes;
    protected final String contractAddress;
    protected final BigInteger fromBlock;
    protected final BigInteger toBlock;
    protected final Event event;


    public EthereumTable( Builder builder ) {
        this.clientUrl = builder.clientUrl;
        this.protoRowType = builder.protoRowType;
        this.fieldTypes = builder.fieldTypes;
        this.fields = builder.fields;
        this.ethereumDataSource = builder.ethereumDataSource;
        this.mapper = builder.mapper;
        this.tableId = builder.tableId;
        this.contractAddress = builder.contractAddress;
        this.fromBlock = builder.fromBlock;
        this.toBlock = builder.toBlock;
        this.event = builder.event;
    }


    // Utilize nested Builder pattern to provide the flexibility of toggling between fetching event data and not fetching it.
    public static class Builder {

        protected final String clientUrl;
        protected final AlgProtoDataType protoRowType;
        protected final int[] fields;
        protected final EthereumDataSource ethereumDataSource;
        protected final EthereumMapper mapper;
        protected List<EthereumFieldType> fieldTypes;
        protected Long tableId;

        private String contractAddress = null;
        private BigInteger fromBlock = null;
        private BigInteger toBlock = null;
        private Event event = null;


        public Builder( String clientUrl,
                AlgProtoDataType protoRowType,
                List<EthereumFieldType> fieldTypes,
                int[] fields,
                EthereumMapper mapper,
                EthereumDataSource ethereumDataSource,
                Long tableId ) {
            this.clientUrl = clientUrl;
            this.protoRowType = protoRowType;
            this.fieldTypes = fieldTypes;
            this.fields = fields;
            this.ethereumDataSource = ethereumDataSource;
            this.mapper = mapper;
            this.tableId = tableId;
        }


        public Builder contractAddress( String val ) {
            this.contractAddress = val;
            return this;
        }


        public Builder fromBlock( BigInteger val ) {
            this.fromBlock = val;
            return this;
        }


        public Builder toBlock( BigInteger val ) {
            this.toBlock = val;
            return this;
        }


        public Builder event( Event val ) {
            this.event = val;
            return this;
        }


        public EthereumTable build() {
            return new EthereumTable( this );
        }

    }


    @Override
    public AlgDataType getRowType( AlgDataTypeFactory typeFactory ) {
        final List<AlgDataType> types = new ArrayList<>();
        final List<String> names = new ArrayList<>();
        for ( AlgDataTypeField field : this.protoRowType.apply( typeFactory ).getFieldList() ) {
            types.add( field.getType() );
            names.add( field.getName() );
        }
        return typeFactory.createStructType( Pair.zip( names, types ) );
    }


    public String toString() {
        return "BlockchainTable";
    }


    @Override
    public Enumerable scan( DataContext dataContext, List<RexNode> filters ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( ethereumDataSource );
        Predicate<BigInteger> blockNumberPredicate = EthereumPredicateFactory.ALWAYS_TRUE;
        if ( ethereumDataSource.isExperimentalFiltering() ) {
            if ( !filters.isEmpty() ) {
                blockNumberPredicate = EthereumPredicateFactory.makePredicate( dataContext, filters, mapper );
            }
        }
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get( dataContext );
        final Predicate<BigInteger> finalBlockNumberPredicate = blockNumberPredicate;

        if ( fields.length == 1 ) {
            return new AbstractEnumerable<Object>() {
                @Override
                public Enumerator<Object> enumerator() {
                    return new EthereumEnumerator<>(
                            clientUrl,
                            ethereumDataSource.getBlocks(),
                            cancelFlag,
                            true,
                            null,
                            mapper,
                            finalBlockNumberPredicate,
                            (EthereumEnumerator.RowConverter<Object>) EthereumEnumerator.converter( fieldTypes, fields ),
                            contractAddress,
                            fromBlock,
                            toBlock,
                            event );
                }
            };
        }
        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new EthereumEnumerator<>(
                        clientUrl,
                        ethereumDataSource.getBlocks(),
                        cancelFlag,
                        true,
                        null,
                        mapper,
                        finalBlockNumberPredicate,
                        (EthereumEnumerator.RowConverter<Object[]>) EthereumEnumerator.converter( fieldTypes, fields ),
                        contractAddress,
                        fromBlock,
                        toBlock,
                        event );
            }
        };
    }

}
