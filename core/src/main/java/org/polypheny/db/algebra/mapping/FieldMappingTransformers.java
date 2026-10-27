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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.entity.mapping.rules.MappingRule;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.rex.RexNode;


/**
 * Register a transformer for each supported field-rule type.
 */
public final class FieldMappingTransformers {

    public static final FieldMappingTransformers DEFAULT = new FieldMappingTransformers( List.of( new DirectColumnTransformer() ) );

    private final Map<Class<? extends MappingRule>, FieldMappingTransformer<?>> transformers;


    public FieldMappingTransformers( List<? extends FieldMappingTransformer<?>> implementations ) {
        Map<Class<? extends MappingRule>, FieldMappingTransformer<?>> registered = new HashMap<>();
        for ( FieldMappingTransformer<?> transformer : implementations ) {
            if ( registered.putIfAbsent( transformer.ruleType(), transformer ) != null ) {
                throw new IllegalArgumentException( "Duplicate mapping transformer for " + transformer.ruleType().getName() );
            }
        }
        transformers = Map.copyOf( registered );
    }


    /**
     * Checks whether a transformer is registered for the mapping rule
     */
    public void requireSupported( MappingRule rule ) {
        if ( !transformers.containsKey( rule.getClass() ) ) {
            throw new GenericRuntimeException( "Unsupported rule type %s.", rule.getClass().getName() );
        }
    }


    /**
     * Create expression for planner node
     */
    public RexNode transform( MappingRule rule, AlgDataTypeField targetField, FieldMappingContext context ) {
        requireSupported( rule );
        RexNode expression = apply( transformers.get( rule.getClass() ), rule, targetField, context );
        if ( expression == null || !targetField.getType().equals( expression.getType() ) ) {
            throw new GenericRuntimeException( "Mapping %d produces an incompatible type for target field %s.", context.getMapping().id(), targetField.getName() );
        }
        return expression;
    }


    /**
     * Create planner node
     */
    private static <R extends MappingRule> RexNode apply(FieldMappingTransformer<R> transformer, MappingRule rule, AlgDataTypeField targetField, FieldMappingContext context ) {
        return transformer.transform( transformer.ruleType().cast( rule ), targetField, context );
    }

}
