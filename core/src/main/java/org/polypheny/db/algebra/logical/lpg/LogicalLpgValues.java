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

package org.polypheny.db.algebra.logical.lpg;

import com.google.common.collect.ImmutableList;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.core.lpg.LpgValues;
import org.polypheny.db.algebra.core.relational.RelationalTransformable;
import org.polypheny.db.algebra.logical.relational.LogicalValues;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.schema.ModelTrait;
import org.polypheny.db.schema.graph.PolyEdge;
import org.polypheny.db.schema.graph.PolyEdge.EdgeDirection;
import org.polypheny.db.schema.graph.PolyNode;
import org.polypheny.db.type.BasicPolyType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Collation;
import org.polypheny.db.util.NlsString;
import org.polypheny.db.util.Pair;


@Getter
public class LogicalLpgValues extends LpgValues implements RelationalTransformable {

    public static final BasicPolyType ID_TYPE = new BasicPolyType( AlgDataTypeSystem.DEFAULT, PolyType.VARCHAR, 36 );
    public static final BasicPolyType LABEL_TYPE = new BasicPolyType( AlgDataTypeSystem.DEFAULT, PolyType.VARCHAR, 255 );
    public static final BasicPolyType VALUE_TYPE = new BasicPolyType( AlgDataTypeSystem.DEFAULT, PolyType.VARCHAR, 255 );
    public static final BasicPolyType NODE_TYPE = new BasicPolyType( AlgDataTypeSystem.DEFAULT, PolyType.NODE );
    public static final BasicPolyType EDGE_TYPE = new BasicPolyType( AlgDataTypeSystem.DEFAULT, PolyType.EDGE );
    private final ImmutableList<ImmutableList<RexLiteral>> values;


    /**
     * Subclass of {@link LpgValues} not targeted at any particular engine or calling convention.
     */
    public LogicalLpgValues( AlgOptCluster cluster, AlgTraitSet traitSet, Collection<PolyNode> nodes, Collection<PolyEdge> edges, ImmutableList<ImmutableList<RexLiteral>> values, AlgDataType rowType ) {
        super( cluster, traitSet, nodes, edges, values, rowType );
        this.values = values;

        assert edges.stream().noneMatch( e -> e.direction == EdgeDirection.NONE ) : "Edges which are created need to have a direction.";

        this.rowType = rowType;
    }


    public static LogicalLpgValues create(
            AlgOptCluster cluster,
            AlgTraitSet traitSet,
            AlgDataType rowType,
            ImmutableList<ImmutableList<RexLiteral>> values ) {
        return new LogicalLpgValues( cluster, traitSet, List.of(), List.of(), values, rowType );
    }


    public static LogicalLpgValues create(
            AlgOptCluster cluster,
            AlgTraitSet traitSet,
            List<Pair<String, PolyNode>> nodes,
            AlgDataType nodeType,
            List<Pair<String, PolyEdge>> edges,
            AlgDataType edgeType ) {

        List<AlgDataTypeField> fields = new ArrayList<>();

        int i = 0;
        for ( String name : Pair.left( nodes ).stream().filter( Objects::nonNull ).collect( Collectors.toList() ) ) {
            fields.add( new AlgDataTypeFieldImpl( name, i, nodeType ) );
            i++;
        }

        for ( String name : Pair.left( edges ).stream().filter( Objects::nonNull ).collect( Collectors.toList() ) ) {
            fields.add( new AlgDataTypeFieldImpl( name, i, edgeType ) );
            i++;
        }

        AlgRecordType rowType = new AlgRecordType( fields );

        return new LogicalLpgValues( cluster, traitSet, Pair.right( nodes ), Pair.right( edges ), ImmutableList.of(), rowType );

    }


    @Override
    public List<AlgNode> getRelationalEquivalent( List<AlgNode> values, List<AlgOptTable> entities, CatalogReader catalogReader ) {
        AlgTraitSet out = traitSet.replace( ModelTrait.RELATIONAL );

        AlgOptCluster cluster = AlgOptCluster.create( getCluster().getPlanner(), getCluster().getRexBuilder() );

        LogicalValues nodeValues = new LogicalValues( cluster, out, entities.get( 0 ).getRowType(), getNodeValues( nodes ) );
        LogicalValues nodePropertyValues = new LogicalValues( cluster, out, entities.get( 1 ).getRowType(), getNodePropertyValues( nodes ) );

        if ( edges.isEmpty() ) {
            return Arrays.asList( nodeValues, nodePropertyValues.tuples.isEmpty() ? null : nodePropertyValues );
        }

        assert entities.size() == 4 && entities.get( 2 ) != null && entities.get( 3 ) != null;
        LogicalValues edgeValues = new LogicalValues( cluster, out, entities.get( 2 ).getRowType(), getEdgeValues( edges ) );
        LogicalValues edgePropertyValues = new LogicalValues( cluster, out, entities.get( 3 ).getRowType(), getEdgePropertyValues( edges ) );

        return Arrays.asList(
                nodeValues,
                nodePropertyValues.tuples.isEmpty() ? null : nodePropertyValues,
                edgeValues,
                edgePropertyValues.tuples.isEmpty() ? null : edgePropertyValues );
    }


    private ImmutableList<ImmutableList<RexLiteral>> getNodeValues( ImmutableList<PolyNode> nodes ) {
        ImmutableList.Builder<ImmutableList<RexLiteral>> rows = ImmutableList.builder();
        for ( PolyNode node : nodes ) {
            RexLiteral id = getNls( node.id, ID_TYPE );
            // empty node without label, as non label nodes are permitted (use $, as null is not possible for pk)
            ImmutableList.Builder<RexLiteral> idRow = ImmutableList.builder();
            idRow.add( id );
            idRow.add( getCluster().getRexBuilder().makeLiteral( "$" ) );
            rows.add( idRow.build() );

            for ( String label : node.labels ) {
                ImmutableList.Builder<RexLiteral> row = ImmutableList.builder();
                row.add( id );
                row.add( getNls( label, LABEL_TYPE ) );
                rows.add( row.build() );
            }
        }
        return rows.build();
    }


    private ImmutableList<ImmutableList<RexLiteral>> getNodePropertyValues( ImmutableList<PolyNode> nodes ) {
        ImmutableList.Builder<ImmutableList<RexLiteral>> rows = ImmutableList.builder();
        for ( PolyNode node : nodes ) {
            RexLiteral id = getNls( node.id, ID_TYPE );

            for ( Entry<String, Object> entry : node.properties.entrySet() ) {
                ImmutableList.Builder<RexLiteral> row = ImmutableList.builder();
                row.add( id );
                row.add( getNls( entry.getKey(), LABEL_TYPE ) );
                row.add( getNls( entry.getValue().toString(), VALUE_TYPE ) );
                rows.add( row.build() );
            }
        }
        return rows.build();
    }


    private static RexLiteral getNls( String value, BasicPolyType type ) {
        return new RexLiteral( new NlsString( value, StandardCharsets.ISO_8859_1.name(), Collation.IMPLICIT ), type, PolyType.CHAR );
    }


    private ImmutableList<ImmutableList<RexLiteral>> getEdgeValues( ImmutableList<PolyEdge> edges ) {
        ImmutableList.Builder<ImmutableList<RexLiteral>> rows = ImmutableList.builder();
        for ( PolyEdge edge : edges ) {
            ImmutableList.Builder<RexLiteral> row = ImmutableList.builder();
            row.add( getNls( edge.id, ID_TYPE ) );
            row.add( getNls( edge.labels.get( 0 ), LABEL_TYPE ) );

            row.add( getNls( edge.source, ID_TYPE ) );
            row.add( getNls( edge.target, ID_TYPE ) );
            rows.add( row.build() );
        }

        return rows.build();
    }


    private ImmutableList<ImmutableList<RexLiteral>> getEdgePropertyValues( ImmutableList<PolyEdge> edges ) {
        ImmutableList.Builder<ImmutableList<RexLiteral>> rows = ImmutableList.builder();
        for ( PolyEdge edge : edges ) {
            RexLiteral id = getNls( edge.id, ID_TYPE );

            for ( Entry<String, Object> entry : edge.properties.entrySet() ) {
                ImmutableList.Builder<RexLiteral> row = ImmutableList.builder();
                row.add( id );
                row.add( getNls( entry.getKey(), LABEL_TYPE ) );
                row.add( getNls( entry.getValue().toString(), VALUE_TYPE ) );
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
