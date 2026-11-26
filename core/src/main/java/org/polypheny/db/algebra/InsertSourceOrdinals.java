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

package org.polypheny.db.algebra;


import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import org.polypheny.db.algebra.core.Calc;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.type.PolyType;


/** Carries a source branch's ordinal through a read-only INSERT preview. */
public record InsertSourceOrdinals( AlgNode plan, int branchCount ) {

    private static final String COLUMN_NAME = "insert_branch_ordinal";


    public static InsertSourceOrdinals add( AlgNode source, int firstOrdinal, LeafProject project,
            BiFunction<AlgNode, Integer, Optional<InsertSourceOrdinals>> boundary ) {
        if ( source instanceof Union union && union.all ) {
            List<AlgNode> inputs = new ArrayList<>();
            int nextOrdinal = firstOrdinal;
            for ( AlgNode input : union.getInputs() ) {
                InsertSourceOrdinals child = add( input, nextOrdinal, project, boundary );
                inputs.add( child.plan );
                nextOrdinal += child.branchCount;
            }
            return new InsertSourceOrdinals( union.copy( union.getTraitSet(), inputs, true ), nextOrdinal - firstOrdinal );
        }

        RexBuilder rex = source.getCluster().getRexBuilder();
        if ( source instanceof Project original ) {
            InsertSourceOrdinals input = add( original.getInput(), firstOrdinal, project, boundary );
            List<RexNode> expressions = new ArrayList<>( original.getProjects() );
            expressions.add( rex.makeInputRef( input.plan, original.getInput().getTupleType().getFieldCount() ) );
            return new InsertSourceOrdinals( original.copy( original.getTraitSet(), input.plan, expressions,
                    rowType( source ) ), input.branchCount );
        }
        if ( source instanceof Calc original && original.getProgram().getCondition() == null ) {
            InsertSourceOrdinals input = add( original.getInput(), firstOrdinal, project, boundary );
            List<RexNode> expressions = new ArrayList<>();
            original.getProgram().getProjectList().forEach( ref -> expressions.add( original.getProgram().expandLocalRef( ref ) ) );
            expressions.add( rex.makeInputRef( input.plan, original.getInput().getTupleType().getFieldCount() ) );
            RexProgram program = RexProgram.create( input.plan.getTupleType(), expressions, null, rowType( source ), rex );
            return new InsertSourceOrdinals( original.copy( original.getTraitSet(), input.plan, program ), input.branchCount );
        }

        Optional<InsertSourceOrdinals> crossed = boundary.apply( source, firstOrdinal );
        if ( crossed.isPresent() ) {
            return crossed.get();
        }
        List<RexNode> expressions = new ArrayList<>();
        for ( int i = 0; i < source.getTupleType().getFieldCount(); i++ ) {
            expressions.add( rex.makeInputRef( source, i ) );
        }
        expressions.add( rex.makeExactLiteral( BigDecimal.valueOf( firstOrdinal ), ordinalType( source ) ) );
        return new InsertSourceOrdinals( project.add( source, expressions, rowType( source ) ), 1 );
    }


    private static AlgDataType rowType( AlgNode source ) {
        return source.getCluster().getTypeFactory().builder()
                .addAll( source.getTupleType().getFields() )
                .add( null, COLUMN_NAME, null, ordinalType( source ) )
                .build();
    }


    private static AlgDataType ordinalType( AlgNode source ) {
        return source.getCluster().getTypeFactory().createPolyType( PolyType.INTEGER );
    }


    @FunctionalInterface
    public interface LeafProject {

        AlgNode add( AlgNode input, List<RexNode> expressions, AlgDataType rowType );

    }

}
