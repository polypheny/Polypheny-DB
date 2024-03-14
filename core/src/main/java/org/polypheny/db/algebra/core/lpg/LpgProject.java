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

package org.polypheny.db.algebra.core.lpg;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.type.entity.PolyString;


@Getter
public abstract class LpgProject extends SingleAlg implements LpgAlg {

    protected final List<? extends RexNode> projects;
    protected final List<PolyString> names;


    /**
     * Creates a {@link LpgProject}.
     * {@link ModelTrait#GRAPH} native node of a project.
     */
    protected LpgProject( AlgCluster cluster, AlgTraitSet traits, AlgNode input, List<? extends RexNode> projects, List<PolyString> names ) {
        super( cluster, traits, input );
        this.projects = projects;
        this.names = names;
    }


    @Override
    public String algCompareString() {
        return "$" + getClass().getSimpleName() +
                "$" + projects.hashCode() +
                "$" + input.algCompareString();
    }


    @Override
    public NodeType getNodeType() {
        return NodeType.PROJECT;
    }


    @Override
    protected AlgDataType deriveRowType() {
        List<AlgDataTypeField> fields = new ArrayList<>();
        if ( names != null && projects != null ) {
            int i = 0;
            int index = 0;
            for ( PolyString name : names ) {
                if ( name != null ) {
                    fields.add( new AlgDataTypeFieldImpl( -1L, name.value, index, projects.get( i ).getType() ) );
                    index++;
                }
                i++;
            }
        } else {
            throw new UnsupportedOperationException();
        }

        return new AlgRecordType( fields );
    }


    @Override
    public AlgNode accept( RexShuttle shuttle ) {
        List<RexNode> exp = this.projects.stream().map( p -> (RexNode) p ).collect( Collectors.toList() );
        List<RexNode> exps = shuttle.apply( exp );
        if ( exp == exps ) {
            return this;
        }
        return copy( traitSet, List.of( input ) );
    }

}
