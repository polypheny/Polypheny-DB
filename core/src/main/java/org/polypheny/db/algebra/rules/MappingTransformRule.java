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

package org.polypheny.db.algebra.rules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.logical.common.LogicalMappingTransform;
import org.polypheny.db.algebra.logical.relational.LogicalRelMappingTransform;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.mapping.FieldMappingContext;
import org.polypheny.db.algebra.mapping.FieldMappingTransformers;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.entity.mapping.rules.ColumnMappingRule;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;


/**
 * Makes an allocation's physical scan produce columns in the logical table's expected shape.
 * Applied after routing has selected an allocation.
 */
public final class MappingTransformRule extends AlgOptRule {

    public static final MappingTransformRule REL_INSTANCE = new MappingTransformRule( LogicalRelMappingTransform.class );

    private final FieldMappingTransformers transformers;


    public MappingTransformRule( Class<? extends LogicalMappingTransform> transformClass ) {
        this( transformClass, FieldMappingTransformers.DEFAULT );
    }


    public MappingTransformRule( Class<? extends LogicalMappingTransform> transformClass, FieldMappingTransformers transformers ) {
        super( operand( transformClass, any() ), AlgFactories.LOGICAL_BUILDER, MappingTransformRule.class.getSimpleName() + "_" + transformClass.getSimpleName() );
        this.transformers = transformers;
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        call.transformTo( apply( call.alg( 0 ) ) );
    }


    public AlgNode apply( LogicalMappingTransform transform ) {
        if ( transform instanceof LogicalRelMappingTransform relational ) {
            return applyRelational( relational );
        }
        throw new GenericRuntimeException( "Unsupported mapping transform type %s.", transform.getClass().getName() );
    }


    private AlgNode applyRelational( LogicalRelMappingTransform transform ) {
        var mapping = transform.getMapping();
        AlgNode input = transform.getInput();

        // create context
        FieldMappingContext context = new FieldMappingContext( mapping, transform.getBinding(), input.getTupleType(), input.getCluster().getRexBuilder() );

        // get rules
        Map<Long, ColumnMappingRule> rules = new HashMap<>();
        for ( var rule : mapping.rules() ) {
            if ( !(rule instanceof ColumnMappingRule columnRule) ) {
                throw new GenericRuntimeException( "Mapping %d has unsupported rule type %s.", mapping.id(), rule.getClass().getName() );
            }
            transformers.requireSupported( columnRule );
            if ( rules.putIfAbsent( columnRule.targetLogicalColumnId(), columnRule ) != null ) {
                throw new GenericRuntimeException( "Mapping %d repeats logical column %d.", mapping.id(), columnRule.targetLogicalColumnId() );
            }
        }

        // Store rules expressions to projects
        // Resolve each logical column's position in the physical scan.
        List<RexNode> projects = new ArrayList<>();
        for ( AlgDataTypeField targetField : transform.getTupleType().getFields() ) {
            ColumnMappingRule rule = targetField.getId() == null ? null : rules.get( targetField.getId() );
            if ( rule == null ) {
                throw new GenericRuntimeException( "Mapping %d has no rule for a requested column of allocation %d.", mapping.id(), transform.getBinding().allocationId() );
            }
            projects.add( transformers.transform( rule, targetField, context ) );
        }

        // If the physical scan already has exactly the required row type and column order, return it.
        if ( input.getTupleType().equals( transform.getTupleType() )
                && input.getTupleType().getFieldIds().equals( transform.getTupleType().getFieldIds() ) && isIdentity( projects ) ) {
            return input;
        }

        // Physical scan returns the right columns in a different order, or returns extra columns.
        // Build a LogicalRelProject that selects and orders the physical fields into the logical row type.
        return LogicalRelProject.create( input, projects, transform.getTupleType() );
    }


    private static boolean isIdentity( List<RexNode> projects ) {
        for ( int i = 0; i < projects.size(); i++ ) {
            if ( !(projects.get( i ) instanceof RexIndexRef ref) || ref.getIndex() != i ) {
                return false;
            }
        }
        return true;
    }

}
