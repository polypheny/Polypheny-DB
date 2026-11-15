/*
 * Copyright 2019-2025 The Polypheny Project
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

import java.util.List;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.core.common.Modify.Operation;
import org.polypheny.db.algebra.core.lpg.LpgTransformer;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.ParamType;
import org.polypheny.db.algebra.polyalg.arguments.EnumArg;
import org.polypheny.db.algebra.polyalg.arguments.ListArg;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArgs;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.GraphType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.type.PolyType;


public class LogicalLpgTransformer extends LpgTransformer {

    /**
     * Subclass of {@link LpgTransformer} not targeted at any particular engine or calling convention.
     */
    public LogicalLpgTransformer( AlgCluster cluster, AlgTraitSet traitSet, List<AlgNode> inputs, AlgDataType rowType, List<PolyType> operationOrder, Operation operation ) {
        super( cluster, traitSet, inputs, rowType, operationOrder, operation );
    }


    public static LogicalLpgTransformer create( List<AlgNode> inputs, List<PolyType> operationOrder, Operation operation ) {
        AlgTraitSet traitSet = inputs.get( 0 ).getTraitSet().replace( AlgCollations.EMPTY );
        AlgDataType type = GraphType.of();
        return new LogicalLpgTransformer( inputs.get( 0 ).getCluster(), traitSet, inputs, type, operationOrder, operation );
    }


    public static LogicalLpgTransformer create( PolyAlgArgs args, List<AlgNode> children, AlgCluster cluster ) {
        EnumArg<Operation> op = args.getEnumArg( "operation", Operation.class );
        List<PolyType> order = args.getListArg( "order", EnumArg.class ).map( e -> (PolyType) e.getArg() );
        return create( children, order, op.getArg() );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new LogicalLpgTransformer( inputs.get( 0 ).getCluster(), traitSet, inputs, rowType, operationOrder, operation );
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }


    @Override
    public PolyAlgArgs bindArguments() {
        PolyAlgArgs args = new PolyAlgArgs( getPolyAlgDeclaration() );

        return args.put( "operation", new EnumArg<>( operation, ParamType.POLY_TYPE_ENUM ) )
                .put( "order", new ListArg<>( operationOrder, o -> new EnumArg<PolyType>( o, ParamType.POLY_TYPE_ENUM ) ) );
    }

}
