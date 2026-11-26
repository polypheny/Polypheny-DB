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
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import org.polypheny.db.algebra.core.Calc;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.algebra.core.relational.RelScan;
import org.polypheny.db.algebra.enumerable.EnumerableLimit;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeUtil;


/** Carries a source branch's ordinal and unassigned source key through a read-only INSERT preview. */
public record InsertSourceOrdinals( AlgNode plan, int branchCount ) {

    private static final String COLUMN_NAME = "insert_branch_ordinal";


    public static InsertSourceOrdinals add( AlgNode source, int firstOrdinal, List<AlgDataTypeField> identityFields,
            LeafProject project, BiFunction<AlgNode, Integer, Optional<InsertSourceOrdinals>> boundary ) {
        if ( source instanceof Union union && union.all ) {
            List<AlgNode> inputs = new ArrayList<>();
            int nextOrdinal = firstOrdinal;
            for ( AlgNode input : union.getInputs() ) {
                InsertSourceOrdinals child = add( input, nextOrdinal, identityFields, project, boundary );
                inputs.add( child.plan );
                nextOrdinal += child.branchCount;
            }
            return new InsertSourceOrdinals( union.copy( union.getTraitSet(), inputs, true ), nextOrdinal - firstOrdinal );
        }
        if ( source instanceof Sort || source instanceof EnumerableLimit ) {
            InsertSourceOrdinals input = add( source.getInput( 0 ), firstOrdinal, identityFields, project, boundary );
            return new InsertSourceOrdinals( source.copy( source.getTraitSet(), List.of( input.plan ) ), input.branchCount );
        }

        RexBuilder rex = source.getCluster().getRexBuilder();
        if ( source instanceof Project original ) {
            InsertSourceOrdinals input = add( original.getInput(), firstOrdinal, identityFields, project, boundary );
            List<RexNode> expressions = new ArrayList<>( original.getProjects() );
            appendSourceMetadata( original.getInput(), input.plan, expressions, rex );
            return new InsertSourceOrdinals( original.copy( original.getTraitSet(), input.plan, expressions,
                    rowType( source, expressions ) ), input.branchCount );
        }
        if ( source instanceof Filter original ) {
            InsertSourceOrdinals input = add( original.getInput(), firstOrdinal, identityFields, project, boundary );
            return new InsertSourceOrdinals( original.copy( original.getTraitSet(), input.plan, original.getCondition() ), input.branchCount );
        }
        if ( source instanceof Calc original ) {
            InsertSourceOrdinals input = add( original.getInput(), firstOrdinal, identityFields, project, boundary );
            List<RexNode> expressions = new ArrayList<>();
            original.getProgram().getProjectList().forEach( ref -> expressions.add( original.getProgram().expandLocalRef( ref ) ) );
            appendSourceMetadata( original.getInput(), input.plan, expressions, rex );
            RexNode condition = original.getProgram().getCondition() == null ? null
                    : original.getProgram().expandLocalRef( original.getProgram().getCondition() );
            RexProgram program = RexProgram.create( input.plan.getTupleType(), expressions, condition, rowType( source, expressions ), rex );
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
        for ( AlgDataTypeField identity : identityFields ) {
            expressions.add( sourceKey( source, identity, rex ) );
        }
        expressions.add( rex.makeExactLiteral( BigDecimal.valueOf( firstOrdinal ), ordinalType( source ) ) );
        return new InsertSourceOrdinals( project.add( source, expressions, rowType( source, expressions ) ), 1 );
    }


    private static void appendSourceMetadata( AlgNode original, AlgNode annotated, List<RexNode> expressions, RexBuilder rex ) {
        for ( int i = original.getTupleType().getFieldCount(); i < annotated.getTupleType().getFieldCount(); i++ ) {
            expressions.add( rex.makeInputRef( annotated, i ) );
        }
    }


    private static RexNode sourceKey( AlgNode source, AlgDataTypeField identity, RexBuilder rex ) {
        AlgDataTypeFactory factory = source.getCluster().getTypeFactory();
        if ( source instanceof RelScan<?> ) {
            for ( AlgDataTypeField field : source.getTupleType().getFields() ) {
                if ( Objects.equals( field.getId(), identity.getId() )
                        && PolyTypeUtil.equalSansNullability( factory, field.getType(), identity.getType() ) ) {
                    return rex.makeInputRef( source, field.getIndex() );
                }
            }
        }
        // Missing, unproven, or unrelated keys have typed NULLs, so UNION branches
        // agree on the metadata shape even when they scan different tables.
        AlgDataType keyType = factory.createTypeWithNullability( identity.getType(), true );
        return rex.makeAbstractCast( keyType, rex.constantNull() );
    }


    private static AlgDataType rowType( AlgNode source, List<RexNode> expressions ) {
        List<AlgDataTypeField> originalFields = source.getTupleType().getFields();
        AlgDataTypeFactory.Builder fields = source.getCluster().getTypeFactory().builder().addAll( originalFields );
        int ordinalIndex = expressions.size() - 1;
        // A scanned primary key retains its exact type, including nullability.
        // Only an unavailable key is nullable. UNION derives the common type.
        for ( int i = originalFields.size(); i < ordinalIndex; i++ ) {
            fields.add( null, "insert_source_key_" + ( i - originalFields.size() ), null, expressions.get( i ).getType() );
        }
        return fields.add( null, COLUMN_NAME, null, expressions.get( ordinalIndex ).getType() ).uniquify().build();
    }


    private static AlgDataType ordinalType( AlgNode source ) {
        return source.getCluster().getTypeFactory().createPolyType( PolyType.INTEGER );
    }


    @FunctionalInterface
    public interface LeafProject {

        AlgNode add( AlgNode input, List<RexNode> expressions, AlgDataType rowType );

    }

}
