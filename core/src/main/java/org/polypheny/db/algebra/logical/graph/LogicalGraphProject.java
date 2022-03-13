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

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.GraphAlg;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.Pair;

@Getter
public class LogicalGraphProject extends SingleAlg implements GraphAlg {

    private final List<? extends RexNode> projects;
    private final List<String> names;


    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traits
     * @param input Input relational expression
     */
    public LogicalGraphProject( AlgOptCluster cluster, AlgTraitSet traits, AlgNode input, List<? extends RexNode> projects, List<String> names ) {
        super( cluster, traits.replace( Convention.NONE ), input );
        assertLogicalGraphTrait( traits );
        this.projects = projects;
        this.names = names;
        assert this.names.size() == this.projects.size();
    }


    @Override
    protected AlgDataType deriveRowType() {
        List<AlgDataTypeField> fields = new ArrayList<>();
        int i = 0;
        for ( Pair<String, ? extends RexNode> pair : Pair.zip( names, projects ) ) {
            fields.add( new AlgDataTypeFieldImpl( pair.left, i, pair.right.getType() ) );
            i++;
        }

        return new AlgRecordType( fields );
    }


    @Override
    public String algCompareString() {
        return "$" + getClass().getSimpleName() + "$" + projects.hashCode();
    }


    @Override
    public NodeType getNodeType() {
        return NodeType.PROJECT;
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new LogicalGraphProject( inputs.get( 0 ).getCluster(), traitSet, inputs.get( 0 ), projects, names );
    }

}
