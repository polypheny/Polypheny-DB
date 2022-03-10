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

package org.polypheny.db.algebra.logical.graph;

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.Getter;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.GraphAlg;
import org.polypheny.db.algebra.logical.LogicalValues;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.schema.ModelTrait;
import org.polypheny.db.schema.graph.PolyEdge;
import org.polypheny.db.schema.graph.PolyNode;
import org.polypheny.db.type.BasicPolyType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;

@Getter
public class LogicalGraphPattern extends AbstractAlgNode implements GraphAlg, RelationalTransformable {

    public static final BasicPolyType ID_TYPE = new BasicPolyType( AlgDataTypeSystem.DEFAULT, PolyType.BIGINT );
    public static final BasicPolyType NODE_TYPE = new BasicPolyType( AlgDataTypeSystem.DEFAULT, PolyType.NODE );
    public static final BasicPolyType EDGE_TYPE = new BasicPolyType( AlgDataTypeSystem.DEFAULT, PolyType.EDGE );
    private final ImmutableList<PolyNode> nodes;
    private final ImmutableList<PolyEdge> edges;


    /**
     * Creates an <code>AbstractRelNode</code>.
     *
     * @param cluster
     * @param traitSet
     */
    public LogicalGraphPattern( AlgOptCluster cluster, AlgTraitSet traitSet, List<PolyNode> nodes, List<PolyEdge> edges, AlgDataType rowType ) {
        super( cluster, traitSet );
        this.nodes = ImmutableList.copyOf( nodes );
        this.edges = ImmutableList.copyOf( edges );

        this.rowType = rowType;
    }


    public static LogicalGraphPattern create(
            AlgOptCluster cluster,
            AlgTraitSet traitSet,
            List<Pair<String, PolyNode>> nodes,
            AlgDataType nodeType,
            List<Pair<String, PolyEdge>> rels,
            AlgDataType relType ) {

        List<AlgDataTypeField> fields = new ArrayList<>();

        int i = 0;
        for ( String name : Pair.left( nodes ) ) {
            fields.add( new AlgDataTypeFieldImpl( name, i, nodeType ) );
            i++;
        }

        for ( String name : Pair.left( rels ) ) {
            fields.add( new AlgDataTypeFieldImpl( name, i, relType ) );
            i++;
        }

        AlgRecordType rowType = new AlgRecordType( fields );

        return new LogicalGraphPattern( cluster, traitSet, Pair.right( nodes ), Pair.right( rels ), rowType );

    }


    @Override
    public String algCompareString() {
        return "$" + getClass().getSimpleName() + "$";
    }


    @Override
    public List<AlgNode> getRelationalEquivalent( List<AlgNode> values ) {
        AlgTraitSet out = traitSet.replace( ModelTrait.RELATIONAL );
        AlgDataTypeFactory typeFactory = getCluster().getTypeFactory();

        AlgOptCluster cluster = AlgOptCluster.create( getCluster().getPlanner(), getCluster().getRexBuilder() );

        AlgDataType arrayType = typeFactory.createArrayType( typeFactory.createPolyType( PolyType.VARCHAR, 255 ), -1, -1 );
        AlgDataTypeField id = new AlgDataTypeFieldImpl( "_id", 0, ID_TYPE );
        AlgDataTypeField node = new AlgDataTypeFieldImpl( "_node", 1, NODE_TYPE );
        AlgDataTypeField edge = new AlgDataTypeFieldImpl( "_edge", 1, EDGE_TYPE );
        AlgDataTypeField labels = new AlgDataTypeFieldImpl( "_labels", 2, arrayType );
        AlgDataTypeField lId = new AlgDataTypeFieldImpl( "_l_id_", 3, ID_TYPE );
        AlgDataTypeField rId = new AlgDataTypeFieldImpl( "_r_id_", 4, ID_TYPE );

        AlgRecordType nodeRowType = new AlgRecordType( Arrays.asList( id, node, labels ) );
        AlgRecordType edgeRowType = new AlgRecordType( Arrays.asList( id, edge, labels, lId, rId ) );

        LogicalValues nodeValues = new LogicalValues( cluster, out, nodeRowType, getNodeValues( nodes, arrayType ) );
        if ( edges.isEmpty() ) {
            return List.of( nodeValues );
        }
        LogicalValues edgeValues = new LogicalValues( cluster, out, edgeRowType, getEdgeValues( edges, arrayType ) );
        return Arrays.asList( nodeValues, edgeValues );
    }


    private ImmutableList<ImmutableList<RexLiteral>> getNodeValues( ImmutableList<PolyNode> nodes, AlgDataType arrayType ) {
        ImmutableList.Builder<ImmutableList<RexLiteral>> rows = ImmutableList.builder();
        for ( PolyNode node : nodes ) {
            ImmutableList.Builder<RexLiteral> row = ImmutableList.builder();
            row.add( new RexLiteral( new BigDecimal( node.id ), ID_TYPE, PolyType.BIGINT ) );
            row.add( new RexLiteral( node, NODE_TYPE, PolyType.NODE ) );
            row.add( new RexLiteral( node.getRexLabels(), arrayType, PolyType.ARRAY ) );
            rows.add( row.build() );
        }

        return rows.build();
    }


    private ImmutableList<ImmutableList<RexLiteral>> getEdgeValues( ImmutableList<PolyEdge> edges, AlgDataType arrayType ) {
        ImmutableList.Builder<ImmutableList<RexLiteral>> rows = ImmutableList.builder();
        for ( PolyEdge edge : edges ) {
            ImmutableList.Builder<RexLiteral> row = ImmutableList.builder();
            row.add( new RexLiteral( new BigDecimal( edge.id ), ID_TYPE, PolyType.BIGINT ) );
            row.add( new RexLiteral( edge, EDGE_TYPE, PolyType.EDGE ) );
            row.add( new RexLiteral( edge.getRexLabels(), arrayType, PolyType.ARRAY ) );
            row.add( new RexLiteral( new BigDecimal( edge.leftId ), ID_TYPE, PolyType.BIGINT ) );
            row.add( new RexLiteral( new BigDecimal( edge.rightId ), ID_TYPE, PolyType.BIGINT ) );
            rows.add( row.build() );
        }

        return rows.build();
    }

}
