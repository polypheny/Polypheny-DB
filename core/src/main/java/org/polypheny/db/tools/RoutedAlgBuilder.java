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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.bson.BsonValue;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptSchema;
import org.polypheny.db.plan.Context;
import org.polypheny.db.plan.Contexts;
import org.polypheny.db.processing.DeepCopyShuttle;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.Pair;


/**
 * Extension of RelBuilder for routed plans with some more information.
 */
public class RoutedAlgBuilder extends AlgBuilder {

    @Getter
    protected Map<Long, List<Pair<Integer, Long>>> physicalPlacementsOfPartitions = new HashMap<>(); // PartitionId -> List<AdapterId, CatalogColumnPlacementId>


    public RoutedAlgBuilder( Context context, AlgOptCluster cluster, AlgOptSchema algOptSchema ) {
        super( context, cluster, algOptSchema );
    }


    public static RoutedAlgBuilder create( Statement statement, AlgOptCluster cluster ) {
        return new RoutedAlgBuilder( Contexts.EMPTY_CONTEXT, cluster, statement.getTransaction().getCatalogReader() );
    }


    public static RoutedAlgBuilder createCopy( Statement statement, AlgOptCluster cluster, RoutedAlgBuilder builder ) {
        final RoutedAlgBuilder newBuilder = RoutedAlgBuilder.create( statement, cluster );
        newBuilder.getPhysicalPlacementsOfPartitions().putAll( ImmutableMap.copyOf( builder.getPhysicalPlacementsOfPartitions() ) );

        if ( builder.stackSize() > 0 ) {
            final AlgNode node = builder.peek().accept( new DeepCopyShuttle() );
            newBuilder.push( node );
        }

        return newBuilder;
    }


    @Override
    public RoutedAlgBuilder values( Iterable<? extends List<RexLiteral>> tupleList, AlgDataType rowType ) {
        super.values( tupleList, rowType );
        return this;
    }


    @Override
    public RoutedAlgBuilder scan( Iterable<String> tableNames ) {
        super.scan( tableNames );
        return this;
    }


    @Override
    public RoutedAlgBuilder push( AlgNode node ) {
        super.push( node );
        return this;
    }


    @Override
    public RoutedAlgBuilder documents( ImmutableList<BsonValue> tuples, AlgDataType rowType ) {
        super.documents( tuples, rowType );
        return this;
    }


    public void addPhysicalInfo( Map<Long, List<CatalogColumnPlacement>> physicalPlacements ) {
        final Map<Long, List<Pair<Integer, Long>>> map = physicalPlacements.entrySet().stream()
                .collect( Collectors.toMap( Map.Entry::getKey, entry -> map( entry.getValue() ) ) );
        physicalPlacementsOfPartitions.putAll( map );
    }


    private List<Pair<Integer, Long>> map( List<CatalogColumnPlacement> catalogCols ) {
        return catalogCols.stream().map( col -> new Pair<>( col.adapterId, col.columnId ) ).collect( Collectors.toList() );
    }


    @AllArgsConstructor
    @Getter
    public static class SelectedAdapterInfo {

        public final String uniqueName;
        public final String physicalSchemaName;
        public final String physicalTableName;

    }

}
