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

package org.polypheny.db.extraction;

import lombok.extern.slf4j.Slf4j;
import org.pf4j.Extension;
import org.pf4j.Plugin;
import org.pf4j.PluginWrapper;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.processing.TransactionExtension;
import org.polypheny.db.transaction.TransactionManager;

public class ExtractionPlugin extends Plugin {

    /**
     * Constructor to be used by plugin manager for plugin instantiation.
     * Your plugins have to provide constructor with this exact signature to be successfully loaded by manager.
     */
    public ExtractionPlugin( PluginWrapper wrapper ) {
        super( wrapper );
    }


    @Override
    public void start() {
        TransactionExtension.REGISTER.add( new ExtractionStarter() );
    }


    @Slf4j
    @Extension
    public static class ExtractionStarter implements TransactionExtension {

        public ExtractionStarter() {
            // empty on purpose
            log.debug( "ExtractionStarter started" );
        }


        @Override
        public void initExtension( TransactionManager manager, Authenticator authenticator ) {
            // Initialize schema extractor
            SchemaExtractor extractor = SchemaExtractor.getInstance();
            extractor.setTransactionManager( manager );
            extractor.startServer( manager );
        }

    }

}
