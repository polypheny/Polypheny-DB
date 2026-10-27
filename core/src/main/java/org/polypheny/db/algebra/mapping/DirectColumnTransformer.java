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

package org.polypheny.db.algebra.mapping;

import java.util.Objects;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.entity.mapping.rules.DirectColumnMappingRule;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.rex.RexNode;


/**
 * Copies an allocation's column unchanged, including an already collection-valued column.
 */
public final class DirectColumnTransformer implements FieldMappingTransformer<DirectColumnMappingRule> {

    @Override
    public Class<DirectColumnMappingRule> ruleType() {
        return DirectColumnMappingRule.class;
    }


    /**
     * Builds the expression describing:
     * the target column receives the source column’s value for each row
     */
    @Override
    public RexNode transform( DirectColumnMappingRule rule, AlgDataTypeField targetField, FieldMappingContext context ) {
        // get source field
        AlgDataTypeField sourceField = context.resolveSourceField( rule.targetLogicalColumnId() );

        // validate
        if ( !Objects.equals( targetField.getId(), rule.targetLogicalColumnId() )
                || !Objects.equals( sourceField.getName(), targetField.getName() )
                || !sourceField.getType().equals( targetField.getType() ) ) {
            throw new GenericRuntimeException( "Mapping %d cannot resolve logical column %d on allocation %d.",
                    context.getMapping().id(), rule.targetLogicalColumnId(), context.getBinding().allocationId() );
        }
        // read this column from the input row
        // this builds part of the query plan
        return context.getRexBuilder().makeInputRef( sourceField.getType(), sourceField.getIndex() );
    }

}
