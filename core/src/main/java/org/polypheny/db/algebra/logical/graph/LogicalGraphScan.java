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


import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.GraphAlg;
import org.polypheny.db.algebra.logical.LogicalScan;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.schema.ModelTrait;

public class LogicalGraphScan extends AbstractAlgNode implements GraphAlg, RelationalTransformable {

    @Getter
    private final LogicalGraph graph;

    @Getter
    private final PolyphenyDbCatalogReader catalogReader;

    @Getter
    @Setter
    private AlgOptTable edgeTable;

    @Getter
    @Setter
    private AlgOptTable nodeTable;


    public LogicalGraphScan( AlgOptCluster cluster, PolyphenyDbCatalogReader catalogReader, AlgTraitSet traitSet, LogicalGraph graph, AlgDataType rowType ) {
        super( cluster, traitSet );
        this.graph = graph;
        this.rowType = rowType;
        this.catalogReader = catalogReader;
    }


    @Override
    public String algCompareString() {
        return "$" + getClass().getSimpleName() + "$" + graph.getNamespaceId();
    }


    @Override
    public List<AlgNode> getRelationalEquivalent( List<AlgNode> inputs, List<AlgOptTable> entities ) {
        assert !entities.isEmpty();
        AlgTraitSet out = getTraitSet().replace( ModelTrait.RELATIONAL );
        LogicalScan nodes = new LogicalScan( getCluster(), out, entities.get( 0 ) );

        if ( entities.size() == 1 ) {
            return List.of( nodes );
        }

        LogicalScan edges = new LogicalScan( getCluster(), out, entities.get( 1 ) );

        return List.of( nodes, edges );
    }


    @Override
    public NodeType getNodeType() {
        return NodeType.SCAN;
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        LogicalGraphScan scan = new LogicalGraphScan( inputs.get( 0 ).getCluster(), catalogReader, traitSet, graph, rowType );
        scan.setEdgeTable( edgeTable );
        scan.setNodeTable( nodeTable );
        return scan;
    }

}
