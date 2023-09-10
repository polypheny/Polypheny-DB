/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.languages;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import lombok.Getter;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.nodes.validate.Validator;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.processing.Processor;

@Getter
public class QueryLanguage {

    private final NamespaceType namespaceType;
    private final String serializedName;
    private final ParserFactory factory;
    private final Supplier<Processor> processorSupplier;
    private final BiFunction<Context, Snapshot, Validator> validatorSupplier;
    private final List<String> otherNames;


    public QueryLanguage( NamespaceType namespaceType, String serializedName, List<String> otherNames, ParserFactory factory, Supplier<Processor> processorSupplier, BiFunction<Context, Snapshot, Validator> validatorSupplier ) {
        this.namespaceType = namespaceType;
        this.serializedName = serializedName;
        this.factory = factory;
        this.processorSupplier = processorSupplier;
        this.validatorSupplier = validatorSupplier;
        this.otherNames = otherNames;
    }


    public static QueryLanguage from( String name ) {
        String normalized = name.toLowerCase( Locale.ROOT );

        return LanguageManager.getLanguages().stream().filter( l -> Objects.equals( l.serializedName, normalized ) || l.otherNames.contains( normalized ) )
                .findFirst()
                .orElseThrow( () -> new RuntimeException( "The query language seems not to be supported!" ) );

    }


    public static boolean containsLanguage( String name ) {
        String normalized = name.toLowerCase( Locale.ROOT );

        return LanguageManager.getLanguages().stream().anyMatch( l -> Objects.equals( l.serializedName, normalized ) );
    }

}
