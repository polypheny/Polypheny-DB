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
 * Copies each physical source document into the logical mapping, preserving its structure and values.
 */
@SerializeRecord
public record DocumentCopyEntireMappingRule() implements MappingRule {

    public static final RuleDefinition<DocumentCopyEntireMappingRule> DEFINITION =
            new RuleDefinition<>( "DOCUMENT_COPY_ENTIRE", "Copy entire document", DocumentCopyEntireMappingRule.class );

}
