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
import org.polypheny.db.algebra.logical.lpg.LogicalLpgMatch;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.type.entity.PolyString;


public abstract class LpgMatch extends SingleAlg implements LpgAlg {

    @Getter
    protected final List<RexCall> matches;
    @Getter
    protected final List<PolyString> names;


    /**
     * Creates a {@link LpgMatch}.
     * {@link ModelTrait#GRAPH} node, which represents a <code>MATCH</code> operator.
     */
    protected LpgMatch( AlgCluster cluster, AlgTraitSet traits, AlgNode input, List<RexCall> matches, List<PolyString> names ) {
        super( cluster, traits, input );
        this.matches = matches;
        this.names = names;
    }


    @Override
    protected AlgDataType deriveRowType() {
        List<AlgDataTypeField> fields = new ArrayList<>();

        int i = 0;
        for ( RexNode match : matches ) {
            fields.add( new AlgDataTypeFieldImpl( -1L, names.get( i ).value, i, match.getType() ) );
            i++;
        }
        return new AlgRecordType( fields );
    }


    @Override
    public String algCompareString() {
        return getClass().getSimpleName() + "$" +
                matches.hashCode() + "$" +
                names.hashCode() + "$" +
                input.algCompareString() + "&";
    }


    @Override
    public NodeType getNodeType() {
        return NodeType.MATCH;
    }


    @Override
    public AlgNode accept( RexShuttle shuttle ) {
        List<RexNode> exp = this.matches.stream().map( m -> (RexNode) m ).collect( Collectors.toList() );
        List<RexNode> exps = shuttle.apply( exp );
        if ( exp == exps ) {
            return this;
        }
        return new LogicalLpgMatch( getCluster(), traitSet, input, exps.stream().map( e -> (RexCall) e ).collect( Collectors.toList() ), names );
    }

}
