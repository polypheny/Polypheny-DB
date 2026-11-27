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
import org.polypheny.db.catalog.entity.mapping.rules.RelCopyColumnMappingRule;
import org.polypheny.db.catalog.exceptions.MappingError;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyTypeUtil;


/**
 * Copies an allocation's column unchanged, including an already collection-valued column.
 */
public final class RelDirectColumnTransformer implements RelFieldMappingTransformer<RelCopyColumnMappingRule> {

    @Override
    public Class<RelCopyColumnMappingRule> ruleType() {
        return RelCopyColumnMappingRule.class;
    }


    /**
     * Builds the expression describing:
     * the target column receives the source column’s value for each row
     */
    @Override
    public RexNode transform( RelCopyColumnMappingRule rule, AlgDataTypeField targetField, RelFieldMappingContext context ) {
        // get source field
        AlgDataTypeField sourceField = context.resolveSourceField( rule.targetLogicalColumnId() );

        // validate
        if ( !Objects.equals( targetField.getId(), rule.targetLogicalColumnId() ) ) {
            throw MappingError.Algebra.targetFieldMismatch( context.getMapping().id(), rule.targetLogicalColumnId(), context.getBinding().allocationId() );
        }
        if ( !Objects.equals( sourceField.getName(), targetField.getName() )
                || !PolyTypeUtil.equalSansNullability( context.getRexBuilder().getTypeFactory(), sourceField.getType(), targetField.getType() ) ) {
            throw MappingError.Incompatible.directColumn( context.getMapping().id(), rule.targetLogicalColumnId(), context.getBinding().allocationId() );
        }
        // read this column from the input row
        // this builds part of the query plan
        if ( sourceField.getType().isNullable() != targetField.getType().isNullable() ) {
            return RelConversionColumnTransformer.project( sourceField, targetField, context );
        }
        return context.getRexBuilder().makeInputRef( sourceField.getType(), sourceField.getIndex() );
    }

}
