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

package org.polypheny.db.catalog.entity.mapping.rules;

import io.activej.serializer.annotations.SerializeRecord;

/**
 * DocumentIdentityMappingRule exposes each physical source document unchanged in the logical collection,
 * preserving all fields, nested objects, arrays, identifiers, and value types.
 * It makes the source document available to query through the logical collection, with its structure and values unchanged.
 */
@SerializeRecord
public record DocumentIdentityMappingRule() implements MappingRule {

    public static final RuleDefinition<DocumentIdentityMappingRule> DEFINITION =
            new RuleDefinition<>( "DOCUMENT_IDENTITY", "Entire document", DocumentIdentityMappingRule.class );

}
