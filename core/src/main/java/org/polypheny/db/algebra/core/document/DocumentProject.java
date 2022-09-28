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

package org.polypheny.db.algebra.core.document;

import java.util.List;
import java.util.stream.Collectors;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;


public abstract class DocumentProject extends SingleAlg implements DocumentAlg {

    public final List<? extends RexNode> projects;


    /**
     * Creates a {@link DocumentProject}.
     * {@link org.polypheny.db.schema.ModelTrait#DOCUMENT} native node of a project.
     */
    protected DocumentProject( AlgOptCluster cluster, AlgTraitSet traits, AlgNode input, List<? extends RexNode> projects, AlgDataType rowType ) {
        super( cluster, traits, input );
        this.projects = projects;
        this.rowType = rowType;
    }


    @Override
    public String algCompareString() {
        return "$" + getClass().getSimpleName() + "$" + projects.hashCode() + "$" + getInput().algCompareString();
    }


    @Override
    public DocType getDocType() {
        return DocType.PROJECT;
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
