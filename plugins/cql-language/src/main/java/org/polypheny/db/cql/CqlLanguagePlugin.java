/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.cql;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.plugins.PluginContext;
import org.polypheny.db.plugins.PolyPlugin;
import org.polypheny.db.plugins.PolyPluginManager;
import org.polypheny.db.webui.crud.LanguageCrud;

@SuppressWarnings("unused")
@Slf4j
public class CqlLanguagePlugin extends PolyPlugin {


    public static final String NAME = "cql";


    /**
     * Constructor to be used by plugin manager for plugin instantiation.
     * Your plugins have to provide constructor with this exact signature to be successfully loaded by manager.
     */
    public CqlLanguagePlugin( PluginContext context ) {
        super( context );
    }


    @Override
    public void start() {
        QueryLanguage language = new QueryLanguage(
                DataModel.RELATIONAL,
                NAME,
                List.of( NAME ),
                null,
                CqlProcessor::new,
                null,
                LanguageManager::toQueryNodes,
                c -> c );
        LanguageManager.getINSTANCE().addQueryLanguage( language );
        PolyPluginManager.AFTER_INIT.add( () -> LanguageCrud.addToResult( language, LanguageCrud::getRelResult ) );
    }


    @Override
    public void stop() {
        QueryLanguage language = QueryLanguage.from( NAME );
        LanguageCrud.deleteToResult( language );
        LanguageManager.removeQueryLanguage( NAME );
    }

}
