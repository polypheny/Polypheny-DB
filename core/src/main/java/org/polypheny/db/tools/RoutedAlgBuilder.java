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

package org.polypheny.db.tools;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.allocation.AllocationColumn;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.Context;
import org.polypheny.db.plan.Contexts;
import org.polypheny.db.processing.DeepCopyShuttle;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.entity.document.PolyDocument;


/**
 * Extension of RelBuilder for routed plans with some more information.
 */
@Getter
public class RoutedAlgBuilder extends AlgBuilder {

    protected Map<Long, List<AllocationColumn>> physicalPlacementsOfPartitions = new HashMap<>(); // PartitionId -> List<AllocationColumn>


    public RoutedAlgBuilder( Context context, AlgOptCluster cluster, Snapshot snapshot ) {
        super( context, cluster, snapshot );
    }


    public static RoutedAlgBuilder create( Statement statement, AlgOptCluster cluster ) {
        return new RoutedAlgBuilder( Contexts.EMPTY_CONTEXT, cluster, statement.getTransaction().getSnapshot() );
    }


    public static RoutedAlgBuilder createCopy( Statement statement, AlgOptCluster cluster, RoutedAlgBuilder builder ) {
        final RoutedAlgBuilder newBuilder = RoutedAlgBuilder.create( statement, cluster );
        newBuilder.getPhysicalPlacementsOfPartitions().putAll( ImmutableMap.copyOf( builder.getPhysicalPlacementsOfPartitions() ) );

        if ( builder.stackSize() > 0 ) {
            for ( int i = 0; i < builder.stackSize(); i++ ) {
                final AlgNode node = builder.peek( i ).accept( new DeepCopyShuttle() );
                newBuilder.push( node );
            }
        }

        return newBuilder;
    }


    @Override
    public RoutedAlgBuilder values( Iterable<? extends List<RexLiteral>> tupleList, AlgDataType rowType ) {
        super.values( tupleList, rowType );
        return this;
    }


    @Override
    public RoutedAlgBuilder scan( List<String> tableNames ) {
        super.scan( tableNames );
        return this;
    }


    @Override
    public RoutedAlgBuilder push( AlgNode node ) {
        super.push( node );
        return this;
    }


    @Override
    public RoutedAlgBuilder documents( List<PolyDocument> documents, AlgDataType rowType ) {
        super.documents( documents, rowType );
        return this;
    }


    public void addPhysicalInfo( Map<Long, List<AllocationColumn>> partitionColumns ) {
        physicalPlacementsOfPartitions.putAll( partitionColumns );
    }



}
