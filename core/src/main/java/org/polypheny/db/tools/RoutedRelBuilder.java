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

package org.polypheny.db.tools;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import org.bson.BsonValue;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.plan.Context;
import org.polypheny.db.plan.Contexts;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptSchema;
import org.polypheny.db.processing.DeepCopyShuttle;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.Pair;


/**
 * Extension of RelBuilder for routed plans with some more information.
 */
public class RoutedRelBuilder extends RelBuilder {

    @Getter
    protected Map<Long, List<Pair<Integer, Long>>> physicalPlacementsOfPartitions = new HashMap<>(); // partitionId, list<adapterId - colId>


    public RoutedRelBuilder( Context context, RelOptCluster cluster, RelOptSchema relOptSchema ) {
        super( context, cluster, relOptSchema );
    }


    public static RoutedRelBuilder create( Statement statement, RelOptCluster cluster ) {
        return new RoutedRelBuilder( Contexts.EMPTY_CONTEXT, cluster, statement.getTransaction().getCatalogReader() );
    }


    public static RoutedRelBuilder createCopy( Statement statement, RelOptCluster cluster, RoutedRelBuilder builder ) {
        final RoutedRelBuilder newBuilder = RoutedRelBuilder.create( statement, cluster );
        newBuilder.getPhysicalPlacementsOfPartitions().putAll( ImmutableMap.copyOf( builder.getPhysicalPlacementsOfPartitions() ) );

        if ( builder.stackSize() > 0 ) {
            final RelNode node = builder.peek().accept( new DeepCopyShuttle() );
            newBuilder.push( node );
        }

        return newBuilder;
    }


    @Override
    public RoutedRelBuilder values( Iterable<? extends List<RexLiteral>> tupleList, RelDataType rowType ) {
        super.values( tupleList, rowType );
        return this;
    }


    @Override
    public RoutedRelBuilder scan( Iterable<String> tableNames ) {
        super.scan( tableNames );
        return this;
    }


    @Override
    public RoutedRelBuilder push( RelNode node ) {
        super.push( node );
        return this;
    }


    @Override
    public RoutedRelBuilder documents( ImmutableList<BsonValue> tuples, RelDataType rowType, ImmutableList<ImmutableList<RexLiteral>> normalizedTuples ) {
        super.documents( tuples, rowType, normalizedTuples );
        return this;
    }


    public RoutedRelBuilder addPhysicalInfo( Map<Long, List<CatalogColumnPlacement>> physicalPlacements ) { // list<adapterId - colId>
        final Map<Long, List<Pair<Integer, Long>>> map = physicalPlacements.entrySet().stream()
                .collect( Collectors.toMap( Map.Entry::getKey, entry -> map( entry.getValue() ) ) );
        physicalPlacementsOfPartitions.putAll( map );
        return this;
    }


    private List<Pair<Integer, Long>> map( List<CatalogColumnPlacement> catalogCols ) {
        return catalogCols.stream().map( col -> new Pair<>( col.adapterId, col.columnId ) ).collect( Collectors.toList() );
    }

}
