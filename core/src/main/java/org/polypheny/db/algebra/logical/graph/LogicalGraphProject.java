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
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexNode;

@Getter
public class LogicalGraphProject extends GraphProject {

    private final List<String> names;


    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traits
     * @param input Input relational expression
     */
    public LogicalGraphProject( AlgOptCluster cluster, AlgTraitSet traits, AlgNode input, List<? extends RexNode> projects, List<String> names ) {
        super( cluster, traits.replace( Convention.NONE ), input, projects );
        assertLogicalGraphTrait( traits );
        this.names = names;
        assert (this.names == null || this.projects == null) || this.names.size() == this.projects.size();
    }


    @Override
    protected AlgDataType deriveRowType() {
        List<AlgDataTypeField> fields = new ArrayList<>();
        if ( names != null && projects != null ) {
            int i = 0;
            int index = 0;
            for ( String name : names ) {
                if ( name != null ) {
                    fields.add( new AlgDataTypeFieldImpl( name, index, projects.get( i ).getType() ) );
                    index++;
                }
                i++;
            }
        } else {
            throw new UnsupportedOperationException();
        }

        return new AlgRecordType( fields );
    }


    public boolean isStar() {
        if ( !projects.stream().allMatch( p -> p.isA( Kind.INPUT_REF ) ) ) {
            return false;
        }

        if ( !(input instanceof LogicalGraphScan) &&
                !((input instanceof LogicalGraphFilter) && ((LogicalGraphFilter) input).getCondition().isAlwaysTrue()) ) {
            return false;
        }

        return true;
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new LogicalGraphProject( inputs.get( 0 ).getCluster(), traitSet, inputs.get( 0 ), projects, names );
    }

}
