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

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import lombok.Getter;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.nodes.validate.Validator;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.processing.Processor;

public class LanguageManager {

    private final PropertyChangeSupport listeners = new PropertyChangeSupport( this );

    @Getter
    private static final LanguageManager INSTANCE = new LanguageManager();

    private static final List<QueryLanguage> REGISTER = new ArrayList<>();


    private LanguageManager() {

    }


    public static List<QueryLanguage> getLanguages() {
        return REGISTER;
    }


    public void addObserver( PropertyChangeListener listener ) {
        listeners.addPropertyChangeListener( listener );
    }


    public void removeObserver( PropertyChangeListener listener ) {
        listeners.removePropertyChangeListener( listener );
    }


    public void addQueryLanguage( NamespaceType namespaceType, String serializedName, List<String> otherNames, ParserFactory factory, Supplier<Processor> processorSupplier, BiFunction<Context, PolyphenyDbCatalogReader, Validator> validatorSupplier ) {
        QueryLanguage language = new QueryLanguage( namespaceType, serializedName, otherNames, factory, processorSupplier, validatorSupplier );
        REGISTER.add( language );
        listeners.firePropertyChange( "language", null, language );
    }


    public static void removeQueryLanguage( String name ) {
        REGISTER.remove( QueryLanguage.from( name ) );
    }

}
