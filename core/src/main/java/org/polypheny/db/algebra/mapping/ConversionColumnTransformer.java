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

import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.entity.mapping.rules.ConversionColumnMappingRule;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.exceptions.MappingExecutionException;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.type.ScalarConversions;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;

/**
 * Turns a column conversion rule into an expression the query can execute.
 */
public final class ConversionColumnTransformer implements FieldMappingTransformer<ConversionColumnMappingRule> {

    @Override
    public Class<ConversionColumnMappingRule> ruleType() {
        return ConversionColumnMappingRule.class;
    }


    @Override
    public RexNode transform( ConversionColumnMappingRule rule, AlgDataTypeField targetField, FieldMappingContext context ) {
        AlgDataTypeField source = context.resolveSourceField( rule.targetLogicalColumnId() );
        AlgDataType targetType = targetField.getType();
        if ( !ConversionColumnMappingRule.supports( source.getType(), targetType ) ) {
            throw new GenericRuntimeException( "Mapping %d cannot convert column '%s' from %s to %s.", context.getMapping().id(), targetField.getName(), source.getType(), targetType );
        }
        return project( source, targetField, context );
    }


    static RexNode project( AlgDataTypeField source, AlgDataTypeField target, FieldMappingContext context ) {
        var rex = context.getRexBuilder();
        RexNode input = rex.makeInputRef( source.getType(), source.getIndex() );
        // build MAPPING_CONVERT expression, that planner adds to projection
        return rex.makeCall( target.getType(), OperatorRegistry.get( OperatorName.MAPPING_CONVERT ), input, rex.makeLiteral( target.getName() ),
                rex.makeLiteral( context.getMapping().name() ), rex.makeLiteral( context.getTargetTableName() ) );
    }


    public static PolyValue convert( PolyValue value, PolyString column, String mappingName, String targetTableName, String sourceType, String targetType,
            PolyType source, PolyType target, int precision, int scale, boolean sameType, boolean nullable, DataContext context ) {
        if ( ScalarConversions.isNull( value ) ) {
            if ( !nullable ) {
                throw MappingExecutionException.nullNotAllowed( mappingName, targetTableName, column.value, sourceType, targetType );
            }
            return null;
        }
        if ( sameType ) {
            return value;
        }
        try {
            return ScalarConversions.convert( value, source, target, precision, scale, context );
        } catch ( RuntimeException e ) {
            throw MappingExecutionException.conversionFailed( mappingName, targetTableName, column.value, value, sourceType, targetType, e );
        }
    }


    public static PolyValue convert( PolyValue value, PolyString column, String mappingName, String targetTableName, AlgDataType source, AlgDataType target, DataContext context ) {
        return convert( value, column, mappingName, targetTableName, source.toString(), target.toString(),
                source.getPolyType(), target.getPolyType(), target.getPrecision(), target.getScale(),
                PolyTypeUtil.equalSansNullability( context.getTypeFactory(), source, target ), target.isNullable(), context );
    }

}
