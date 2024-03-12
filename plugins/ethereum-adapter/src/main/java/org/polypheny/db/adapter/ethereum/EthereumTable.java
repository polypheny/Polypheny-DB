/*
 * Copyright 2019-2024 The Polypheny Project
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
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.ethereum.EthereumPlugin.EthereumDataSource;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.types.FilterableEntity;
import org.polypheny.db.type.entity.PolyValue;

public class EthereumTable extends PhysicalTable implements FilterableEntity {

    protected final String clientUrl;
    protected final AlgProtoDataType protoRowType;
    protected final int[] fields;
    protected final EthereumDataSource ethereumDataSource;
    protected final EthereumMapper mapper;
    protected List<EthereumFieldType> fieldTypes;


    public EthereumTable(
            PhysicalTable table,
            String clientUrl,
            AlgProtoDataType protoRowType,
            List<EthereumFieldType> fieldTypes,
            int[] fields,
            EthereumMapper mapper,
            EthereumDataSource ethereumDataSource ) {
        super( table.id,
                table.allocationId,
                table.logicalId,
                table.name,
                table.columns,
                table.namespaceId,
                table.namespaceName,
                table.uniqueFieldIds,
                table.adapterId );
        this.clientUrl = clientUrl;
        this.protoRowType = protoRowType;
        this.fieldTypes = fieldTypes;
        this.fields = fields;
        this.ethereumDataSource = ethereumDataSource;
        this.mapper = mapper;
    }


    @Override
    public AlgDataType getTupleType( AlgDataTypeFactory typeFactory ) {
        return typeFactory.createStructType( this.protoRowType.apply( typeFactory ).getFields() );
    }


    public String toString() {
        return "BlockchainTable";
    }


    @Override
    public Enumerable<PolyValue[]> scan( DataContext dataContext, List<RexNode> filters ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( ethereumDataSource );
        Predicate<BigInteger> blockNumberPredicate = EthereumPredicateFactory.ALWAYS_TRUE;
        if ( ethereumDataSource.isExperimentalFiltering() ) {
            if ( !filters.isEmpty() ) {
                blockNumberPredicate = EthereumPredicateFactory.makePredicate( dataContext, filters, mapper );
            }
        }
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get( dataContext );
        final Predicate<BigInteger> finalBlockNumberPredicate = blockNumberPredicate;

        return new AbstractEnumerable<PolyValue[]>() {
            @Override
            public Enumerator<PolyValue[]> enumerator() {
                return new EthereumEnumerator(
                        clientUrl,
                        ethereumDataSource.getBlocks(),
                        cancelFlag,
                        true,
                        null,
                        mapper,
                        finalBlockNumberPredicate,
                        EthereumEnumerator.converter( fieldTypes, fields ) );
            }
        };
    }

}
