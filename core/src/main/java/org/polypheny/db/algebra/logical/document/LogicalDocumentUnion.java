/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.algebra.logical.document;

import java.util.List;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.algebra.polyalg.arguments.BooleanArg;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArgs;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.schema.trait.ModelTrait;


public final class LogicalDocumentUnion extends Union {

    private LogicalDocumentUnion( AlgCluster cluster, AlgTraitSet traits, List<AlgNode> inputs, boolean all ) {
        super( cluster, traits, inputs, all );
    }


    public static LogicalDocumentUnion create( List<AlgNode> inputs, boolean all ) {
        var cluster = inputs.get( 0 ).getCluster();
        return new LogicalDocumentUnion( cluster, cluster.traitSetOf( Convention.NONE ).replace( ModelTrait.DOCUMENT ), inputs, all );
    }


    public static LogicalDocumentUnion create( PolyAlgArgs args, List<AlgNode> inputs, AlgCluster cluster ) {
        return create( inputs, args.getArg( "all", BooleanArg.class ).toBool() );
    }


    @Override
    protected AlgDataType deriveRowType() {
        AlgDataType sharedType = sharedInputRowType( inputs );
        return sharedType != null ? sharedType : super.deriveRowType();
    }


    public static @Nullable AlgDataType sharedInputRowType( List<AlgNode> inputs ) {
        if ( inputs.isEmpty() ) {
            return null;
        }
        AlgDataType type = inputs.get( 0 ).getTupleType();
        return inputs.stream().allMatch( input -> type.equals( input.getTupleType() ) ) ? type : null;
    }


    @Override
    public LogicalDocumentUnion copy( AlgTraitSet traits, List<AlgNode> inputs, boolean all ) {
        return new LogicalDocumentUnion( getCluster(), traits, inputs, all );
    }

}
