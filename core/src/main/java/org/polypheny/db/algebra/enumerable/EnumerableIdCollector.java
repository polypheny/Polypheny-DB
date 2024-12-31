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

package org.polypheny.db.algebra.enumerable;

import java.util.List;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgProducingVisitor.Function3;
import org.polypheny.db.algebra.core.common.IdentifierCollector;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.transaction.TransactionManagerProvider;
import org.polypheny.db.transaction.locking.IdentifierUtils;
import org.polypheny.db.transaction.locking.VersionedEntryIdentifier;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.graph.GraphPropertyHolder;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.util.BuiltInMethod;

public class EnumerableIdCollector extends IdentifierCollector implements EnumerableAlg {

    protected EnumerableIdCollector( AlgCluster cluster, AlgTraitSet traits, Transaction transaction, Entity entity, AlgNode input ) {
        super( cluster, traits, transaction, entity, input );
        assert getConvention() instanceof EnumerableConvention;
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        double dRows = mq.getTupleCount( getInput() );
        return planner.getCostFactory().makeCost( dRows, 0, 0 );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new EnumerableIdCollector( inputs.get( 0 ).getCluster(), traitSet, transaction, entity, inputs.get( 0 ) );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        final BlockBuilder builder = new BlockBuilder();
        final EnumerableAlg input = (EnumerableAlg) getInput();
        final Result result = implementor.visitChild( this, 0, input, pref );
        final PhysType physType = result.physType();

        Expression input_ = builder.append( "input", result.block() );
        Expression entityId_ = Expressions.constant( entity.getId() );
        Expression transactionId_ = Expressions.constant( transaction.getId() );
        Expression collection_ = null;
        switch ( input.getModel() ) {
            case RELATIONAL -> {
                Expression collectRelIdentifiers_ = Expressions.call( EnumerableIdCollector.class, "collectRelIdentifiers" );
                Expression collectRelIdentifiersWithTx_ = Expressions.call( EnumerableIdCollector.class, "bindTransactionAndLogicalId", collectRelIdentifiers_, transactionId_, entityId_ );
                collection_ = builder.append( "collection", Expressions.call( BuiltInMethod.PROCESS_AND_STREAM_RIGHT.method, input_, collectRelIdentifiersWithTx_ ) );
            }
            case DOCUMENT -> {
                Expression collectDocIdentifiers_ = Expressions.call( EnumerableIdCollector.class, "collectDocIdentifiers" );
                Expression collectDocIdentifiersWithTx_ = Expressions.call( EnumerableIdCollector.class, "bindTransactionAndLogicalId", collectDocIdentifiers_, transactionId_, entityId_ );
                collection_ = builder.append( "collection", Expressions.call( BuiltInMethod.PROCESS_AND_STREAM_RIGHT.method, input_, collectDocIdentifiersWithTx_ ) );
            }
            case GRAPH -> {
                Expression collectLpgIdentifiers_ = Expressions.call( EnumerableIdCollector.class, "collectLpgIdentifiers" );
                Expression collectLpgIdentifiersWithTx_ = Expressions.call( EnumerableIdCollector.class, "bindTransactionAndLogicalId", collectLpgIdentifiers_, transactionId_, entityId_ );
                collection_ = builder.append( "collection", Expressions.call( BuiltInMethod.PROCESS_AND_STREAM_RIGHT.method, input_, collectLpgIdentifiersWithTx_ ) );
            }
        }
        assert collection_ != null;
        builder.add( Expressions.return_( null, collection_ ) );
        return implementor.result( physType, builder.toBlock() );
    }


    public static Function1<Enumerable<PolyValue[]>, Enumerable<PolyValue[]>> bindTransactionAndLogicalId(
            Function3<Enumerable<PolyValue[]>, Transaction, Long, Enumerable<PolyValue[]>> function,
            long transactionId,
            long logicalId ) {
        Transaction transaction = TransactionManagerProvider.getInstance().getTransactionById( transactionId );
        return input -> function.apply( input, transaction, logicalId );
    }


    public static Function3<Enumerable<PolyValue[]>, Transaction, Long, Enumerable<PolyValue[]>> collectRelIdentifiers() {
        return ( input, transaction, logicalId ) -> {
            input.forEach( row -> {
                if ( !(row[0] instanceof PolyLong entryIdentifier) ) {
                    return;
                }
                transaction.addReadEntity( new VersionedEntryIdentifier( logicalId, entryIdentifier.getValue() ) );
            } );
            return input;
        };
    }


    public static Function3<Enumerable<PolyValue[]>, Transaction, Long, Enumerable<PolyValue[]>> collectDocIdentifiers() {
        return ( input, transaction, logicalId ) -> {
            input.forEach( row -> {
                for ( PolyValue value : row ) {
                    if ( !(value instanceof PolyDocument) ) {
                        continue;
                    }
                    PolyValue identifier = ((PolyDocument) value).get( IdentifierUtils.getIdentifierKeyAsPolyString() );
                    if ( !(identifier instanceof PolyLong entryIdentifier) ) {
                        continue;
                    }
                    transaction.addReadEntity( new VersionedEntryIdentifier( logicalId, entryIdentifier.getValue() ) );
                }
            } );
            return input;
        };
    }


    public static Function3<Enumerable<PolyValue[]>, Transaction, Long, Enumerable<PolyValue[]>> collectLpgIdentifiers() {
        return ( input, transaction, logicalId ) -> {
            input.forEach( row -> {
                for ( PolyValue value : row ) {
                    if ( !(value instanceof GraphPropertyHolder) ) {
                        continue;
                    }
                    PolyValue identifier = ((GraphPropertyHolder) value).getProperties()
                            .get( IdentifierUtils.getIdentifierKeyAsPolyString() );
                    if ( !(identifier instanceof PolyLong entryIdentifier) ) {
                        continue;
                    }
                    transaction.addReadEntity( new VersionedEntryIdentifier( logicalId, entryIdentifier.getValue() ) );
                }
            } );
            return input;
        };
    }

}
