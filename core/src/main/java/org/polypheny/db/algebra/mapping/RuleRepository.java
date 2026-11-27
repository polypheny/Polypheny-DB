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

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.catalog.entity.mapping.rules.RelConvertColumnMappingRule;
import org.polypheny.db.catalog.entity.mapping.rules.RelCopyColumnMappingRule;
import org.polypheny.db.catalog.entity.mapping.rules.DocumentCopyEntireMappingRule;
import org.polypheny.db.catalog.entity.mapping.rules.MappingRule;
import org.polypheny.db.catalog.entity.mapping.rules.RuleDefinition;

/**
 * Handles all existing mapping rules
 */
public final class RuleRepository {

    private static final List<Registration<?>> RULES = List.of(
            new Registration<>( RelCopyColumnMappingRule.DEFINITION, new RelDirectColumnTransformer() ),
            new Registration<>( RelConvertColumnMappingRule.DEFINITION, new RelConversionColumnTransformer() ),
            new Registration<>( DocumentCopyEntireMappingRule.DEFINITION ) );

    private static final List<RuleDefinition<?>> DEFINITIONS = RULES.stream().<RuleDefinition<?>>map( Registration::definition ).toList();


    private RuleRepository() {
    }


    public static List<RuleDefinition<?>> definitions() {
        return DEFINITIONS;
    }


    public static Optional<RuleDefinition<?>> definitionFor( Class<? extends MappingRule> ruleClass ) {
        return DEFINITIONS.stream().filter( definition -> definition.ruleClass().equals( ruleClass ) ).findFirst();
    }


    public static Optional<RuleDefinition<?>> definitionForId( String id ) {
        return DEFINITIONS.stream().filter( definition -> definition.id().equals( id ) ).findFirst();
    }


    public static List<RelFieldMappingTransformer<?>> relFieldTransformers() {
        return RULES.stream().<RelFieldMappingTransformer<?>>flatMap( rule -> Stream.ofNullable( rule.relTransformer() ) ).toList();
    }


    private record Registration<R extends MappingRule>( RuleDefinition<R> definition, @Nullable RelFieldMappingTransformer<R> relTransformer ) {

        private Registration( RuleDefinition<R> definition ) {
            this( definition, null );
        }

    }

}
