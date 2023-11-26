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
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import lombok.Value;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.nodes.validate.Validator;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;

@Value
public class QueryLanguage {

    @NotNull
    NamespaceType namespaceType;
    @NotNull
    String serializedName;
    @NotNull
    List<String> otherNames;
    @Nullable
    ParserFactory factory;
    @NotNull
    Supplier<Processor> processorSupplier;
    @Nullable
    BiFunction<Context, Snapshot, Validator> validatorSupplier;
    @NotNull
    Function<QueryContext, List<ParsedQueryContext>> splitter;


    public QueryLanguage(
            @NotNull NamespaceType namespaceType,
            @NotNull String serializedName,
            @NotNull List<String> otherNames,
            @Nullable ParserFactory factory,
            @NotNull Supplier<Processor> processorSupplier,
            @Nullable BiFunction<Context, Snapshot, Validator> validatorSupplier,
            @NotNull Function<QueryContext, List<ParsedQueryContext>> splitter ) {
        this.namespaceType = namespaceType;
        this.serializedName = serializedName;
        this.factory = factory;
        this.processorSupplier = processorSupplier;
        this.validatorSupplier = validatorSupplier;
        this.otherNames = otherNames;
        this.splitter = splitter;
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
