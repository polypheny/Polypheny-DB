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
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.schema.graph.PolyNode;
import org.polypheny.db.schema.graph.PolyRelationship;
import org.polypheny.db.util.Pair;

@Getter
public class LogicalGraphPattern extends AbstractAlgNode {

    private final ImmutableList<PolyNode> nodes;
    private final ImmutableList<PolyRelationship> rels;


    /**
     * Creates an <code>AbstractRelNode</code>.
     *
     * @param cluster
     * @param traitSet
     */
    public LogicalGraphPattern( AlgOptCluster cluster, AlgTraitSet traitSet, List<PolyNode> nodes, List<PolyRelationship> rels, AlgDataType rowType ) {
        super( cluster, traitSet );
        this.nodes = ImmutableList.copyOf( nodes );
        this.rels = ImmutableList.copyOf( rels );

        this.rowType = rowType;
    }


    public static LogicalGraphPattern create(
            AlgOptCluster cluster,
            AlgTraitSet traitSet,
            List<Pair<String, PolyNode>> nodes,
            AlgDataType nodeType,
            List<Pair<String, PolyRelationship>> rels,
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

}
