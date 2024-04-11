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

package org.polypheny.db.algebra.logical.lpg;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import lombok.Getter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.core.lpg.LpgValues;
import org.polypheny.db.algebra.core.relational.RelationalTransformable;
import org.polypheny.db.algebra.logical.relational.LogicalRelValues;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.type.BasicPolyType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyEdge.EdgeDirection;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.util.Collation;
import org.polypheny.db.util.Pair;


@Getter
public class LogicalLpgValues extends LpgValues implements RelationalTransformable {

    public static final BasicPolyType ID_TYPE = new BasicPolyType( AlgDataTypeSystem.DEFAULT, PolyType.VARCHAR, 36 )
            .createWithCharsetAndCollation( Charsets.UTF_8, Collation.IMPLICIT );
    public static final BasicPolyType LABEL_TYPE = new BasicPolyType( AlgDataTypeSystem.DEFAULT, PolyType.VARCHAR, 255 )
            .createWithCharsetAndCollation( Charsets.UTF_8, Collation.IMPLICIT );
    public static final BasicPolyType VALUE_TYPE = new BasicPolyType( AlgDataTypeSystem.DEFAULT, PolyType.VARCHAR, 255 )
            .createWithCharsetAndCollation( Charsets.UTF_8, Collation.IMPLICIT );
    public static final BasicPolyType NODE_TYPE = new BasicPolyType( AlgDataTypeSystem.DEFAULT, PolyType.NODE );
    public static final BasicPolyType EDGE_TYPE = new BasicPolyType( AlgDataTypeSystem.DEFAULT, PolyType.EDGE );
    private final ImmutableList<ImmutableList<RexLiteral>> values;


    /**
     * Subclass of {@link LpgValues} not targeted at any particular engine or calling convention.
     */
    public LogicalLpgValues( AlgCluster cluster, AlgTraitSet traitSet, Collection<PolyNode> nodes, Collection<PolyEdge> edges, ImmutableList<ImmutableList<RexLiteral>> values, AlgDataType rowType ) {
        super( cluster, traitSet, nodes, edges, values, rowType );
        this.values = values;

        assert edges.stream().noneMatch( e -> e.direction == EdgeDirection.NONE ) : "Edges which are created need to have a direction.";

        this.rowType = rowType;
    }


    public static LogicalLpgValues create(
            AlgCluster cluster,
            AlgTraitSet traitSet,
            AlgDataType rowType,
            ImmutableList<ImmutableList<RexLiteral>> values ) {
        return new LogicalLpgValues( cluster, traitSet, List.of(), List.of(), values, rowType );
    }


    public static LogicalLpgValues create(
            AlgCluster cluster,
            AlgTraitSet traitSet,
            List<Pair<PolyString, PolyNode>> nodes,
            AlgDataType nodeType,
            List<Pair<PolyString, PolyEdge>> edges,
            AlgDataType edgeType ) {

        List<AlgDataTypeField> fields = new ArrayList<>();

        int i = 0;
        for ( PolyString name : Pair.left( nodes ).stream().filter( Objects::nonNull ).toList() ) {
            fields.add( new AlgDataTypeFieldImpl( -1L, name.value, i, nodeType ) );
            i++;
        }

        for ( PolyString name : Pair.left( edges ).stream().filter( s -> s.value != null ).toList() ) {
            fields.add( new AlgDataTypeFieldImpl( -1L, name.value, i, edgeType ) );
            i++;
        }

        AlgRecordType rowType = new AlgRecordType( fields );

        return new LogicalLpgValues( cluster, traitSet, Pair.right( nodes ), Pair.right( edges ), ImmutableList.of(), rowType );

    }


    @Override
    public List<AlgNode> getRelationalEquivalent( List<AlgNode> values, List<Entity> entities, Snapshot snapshot ) {
        AlgTraitSet out = traitSet.replace( ModelTrait.RELATIONAL );

        AlgCluster cluster = AlgCluster.create( getCluster().getPlanner(), getCluster().getRexBuilder(), out, snapshot );

        LogicalRelValues nodeValues = new LogicalRelValues( cluster, out, entities.get( 0 ).getTupleType(), getNodeValues( nodes ) );
        LogicalRelValues nodePropertyValues = new LogicalRelValues( cluster, out, entities.get( 1 ).getTupleType(), getNodePropertyValues( nodes ) );

        if ( edges.isEmpty() ) {
            return Arrays.asList( nodeValues, nodePropertyValues.tuples.isEmpty() ? null : nodePropertyValues );
        }

        assert entities.size() == 4 && entities.get( 2 ) != null && entities.get( 3 ) != null;
        LogicalRelValues edgeValues = new LogicalRelValues( cluster, out, entities.get( 2 ).getTupleType(), getEdgeValues( edges ) );
        LogicalRelValues edgePropertyValues = new LogicalRelValues( cluster, out, entities.get( 3 ).getTupleType(), getEdgePropertyValues( edges ) );

        return Arrays.asList(
                nodeValues,
                nodePropertyValues.tuples.isEmpty() ? null : nodePropertyValues,
                edgeValues,
                edgePropertyValues.tuples.isEmpty() ? null : edgePropertyValues );
    }


    private ImmutableList<ImmutableList<RexLiteral>> getNodeValues( ImmutableList<PolyNode> nodes ) {
        ImmutableList.Builder<ImmutableList<RexLiteral>> rows = ImmutableList.builder();
        for ( PolyNode node : nodes ) {
            RexLiteral id = getStringLiteral( node.id.value, ID_TYPE );
            // empty node without label, as non label nodes are permitted (use $, as null is not possible for pk)
            ImmutableList.Builder<RexLiteral> idRow = ImmutableList.builder();
            idRow.add( id );
            idRow.add( getCluster().getRexBuilder().makeLiteral( "$" ) );
            rows.add( idRow.build() );

            for ( PolyString label : node.labels ) {
                ImmutableList.Builder<RexLiteral> row = ImmutableList.builder();
                row.add( id );
                row.add( getStringLiteral( label.value, LABEL_TYPE ) );
                rows.add( row.build() );
            }
        }
        return rows.build();
    }


    private ImmutableList<ImmutableList<RexLiteral>> getNodePropertyValues( ImmutableList<PolyNode> nodes ) {
        ImmutableList.Builder<ImmutableList<RexLiteral>> rows = ImmutableList.builder();
        for ( PolyNode node : nodes ) {
            RexLiteral id = getStringLiteral( node.id.value, ID_TYPE );

            for ( Entry<PolyString, PolyValue> entry : node.properties.entrySet() ) {
                ImmutableList.Builder<RexLiteral> row = ImmutableList.builder();
                row.add( id );
                row.add( getStringLiteral( entry.getKey().value, LABEL_TYPE ) );
                row.add( getStringLiteral( entry.getValue().toString(), VALUE_TYPE ) );
                rows.add( row.build() );
            }
        }
        return rows.build();
    }


    private static RexLiteral getStringLiteral( String value, BasicPolyType type ) {
        return new RexLiteral( PolyString.of( value ), type, PolyType.VARCHAR );
    }


    private ImmutableList<ImmutableList<RexLiteral>> getEdgeValues( ImmutableList<PolyEdge> edges ) {
        ImmutableList.Builder<ImmutableList<RexLiteral>> rows = ImmutableList.builder();
        for ( PolyEdge edge : edges ) {
            ImmutableList.Builder<RexLiteral> row = ImmutableList.builder();
            row.add( getStringLiteral( edge.id.value, ID_TYPE ) );
            row.add( getStringLiteral( edge.labels.get( 0 ).value, LABEL_TYPE ) );

            row.add( getStringLiteral( edge.source.value, ID_TYPE ) );
            row.add( getStringLiteral( edge.target.value, ID_TYPE ) );
            rows.add( row.build() );
        }

        return rows.build();
    }


    private ImmutableList<ImmutableList<RexLiteral>> getEdgePropertyValues( ImmutableList<PolyEdge> edges ) {
        ImmutableList.Builder<ImmutableList<RexLiteral>> rows = ImmutableList.builder();
        for ( PolyEdge edge : edges ) {
            RexLiteral id = getStringLiteral( edge.id.value, ID_TYPE );

            for ( Entry<PolyString, PolyValue> entry : edge.properties.entrySet() ) {
                ImmutableList.Builder<RexLiteral> row = ImmutableList.builder();
                row.add( id );
                row.add( getStringLiteral( entry.getKey().value, LABEL_TYPE ) );
                row.add( getStringLiteral( entry.getValue().toString(), VALUE_TYPE ) );
                rows.add( row.build() );
            }
        }
        return rows.build();
    }


    public boolean isEmptyGraphValues() {
        return edges.isEmpty() && nodes.isEmpty();
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }

}
