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

import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.entity.mapping.rules.MappingRule;
import org.polypheny.db.rex.RexNode;


/**
 * Compiles one mapping rule into a value expression during planning.
 * Each transformer produces a value for one logical target column.
 */
public interface FieldMappingTransformer<R extends MappingRule> {

    Class<R> ruleType();

    RexNode transform( R rule, AlgDataTypeField targetField, FieldMappingContext context );

}
