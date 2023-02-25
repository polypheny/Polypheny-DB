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

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import lombok.Getter;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.nodes.validate.Validator;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.processing.Processor;

public class QueryLanguage {

    @Getter
    private final NamespaceType namespaceType;
    @Getter
    private final String serializedName;
    @Getter
    private final ParserFactory factory;
    @Getter
    private final Supplier<Processor> processorSupplier;
    @Getter
    private final BiFunction<Context, PolyphenyDbCatalogReader, Validator> validatorSupplier;
    @Getter
    private final List<String> otherNames;


    public QueryLanguage( NamespaceType namespaceType, String serializedName, List<String> otherNames, ParserFactory factory, Supplier<Processor> processorSupplier, BiFunction<Context, PolyphenyDbCatalogReader, Validator> validatorSupplier ) {
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


    public static TypeAdapter<QueryLanguage> getSerializer() {

        return new TypeAdapter<>() {

            @Override
            public void write( JsonWriter out, QueryLanguage value ) throws IOException {
                out.value( value.serializedName );
            }


            @Override
            public QueryLanguage read( JsonReader in ) throws IOException {
                return QueryLanguage.from( in.nextString() );
            }
        };
    }

}
