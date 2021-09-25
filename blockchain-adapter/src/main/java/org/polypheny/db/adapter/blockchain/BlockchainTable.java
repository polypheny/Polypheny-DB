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

package org.polypheny.db.adapter.blockchain;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rel.type.RelProtoDataType;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.FilterableTable;
import org.polypheny.db.schema.impl.AbstractTable;
import org.polypheny.db.util.Pair;

public class BlockchainTable extends AbstractTable implements FilterableTable {

    protected final String clientUrl;
    protected final RelProtoDataType protoRowType;
    protected final int[] fields;
    protected final BlockchainDataSource blockchainDataSource;
    protected final BlockchainMapper mapper;
    private final int blocks;
    private final boolean experimentalFiltering;
    protected List<BlockchainFieldType> fieldTypes;


    public BlockchainTable( String clientUrl, int blocks, boolean experimentalFiltering, RelProtoDataType protoRowType, List<BlockchainFieldType> fieldTypes, int[] fields, BlockchainMapper mapper, BlockchainDataSource blockchainDataSource ) {
        this.clientUrl = clientUrl;
        this.protoRowType = protoRowType;
        this.fieldTypes = fieldTypes;
        this.fields = fields;
        this.blockchainDataSource = blockchainDataSource;
        this.mapper = mapper;
        this.blocks = blocks;
        this.experimentalFiltering = experimentalFiltering;
    }


    @Override
    public RelDataType getRowType( RelDataTypeFactory typeFactory ) {
        final List<RelDataType> types = new ArrayList<>();
        final List<String> names = new ArrayList<>();
        for ( RelDataTypeField field : this.protoRowType.apply( typeFactory ).getFieldList() ) {
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
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( blockchainDataSource );
        Predicate<BigInteger> blockNumberPredicate = BlockchainPredicateFactory.ALWAYS_TRUE;
        if ( experimentalFiltering ) {
            if ( !filters.isEmpty() ) {
                blockNumberPredicate = BlockchainPredicateFactory.makePredicate( dataContext, filters, mapper );
            }
        }
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get( dataContext );
        final Predicate<BigInteger> finalBlockNumberPredicate = blockNumberPredicate;

        if ( fields.length == 1 ) {
            return new AbstractEnumerable<Object>() {
                @Override
                public Enumerator<Object> enumerator() {
                    return new BlockchainEnumerator<>( clientUrl, blocks, cancelFlag, true, null, mapper, finalBlockNumberPredicate, (BlockchainEnumerator.RowConverter<Object>) BlockchainEnumerator.converter( fieldTypes, fields ) );
                }
            };
        }
        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new BlockchainEnumerator<>( clientUrl, blocks, cancelFlag, true, null, mapper, finalBlockNumberPredicate, (BlockchainEnumerator.RowConverter<Object[]>) BlockchainEnumerator.converter( fieldTypes, fields ) );
            }
        };
    }

}
