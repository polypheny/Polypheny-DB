/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.exploreByExample;

import lombok.extern.slf4j.Slf4j;
import org.pf4j.Extension;
import org.pf4j.Plugin;
import org.pf4j.PluginWrapper;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.processing.TransactionExtension;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.webui.HttpServer;
import org.polypheny.db.webui.HttpServer.HandlerType;

public class ExploreByExamplePlugin extends Plugin {

    /**
     * Constructor to be used by plugin manager for plugin instantiation.
     * Your plugins have to provide constructor with this exact signature to
     * be successfully loaded by manager.
     *
     * @param wrapper
     */
    public ExploreByExamplePlugin( PluginWrapper wrapper ) {
        super( wrapper );
    }


    @Override
    public void start() {
        TransactionExtension.REGISTER.add( new ExploreStarter() );
    }


    @Slf4j
    @Extension
    public static class ExploreStarter implements TransactionExtension {

        public ExploreStarter() {
            // empty on purpose
            log.info( "ExploreStarter" );
        }


        @Override
        public void initExtension( TransactionManager manager, Authenticator authenticator ) {
            final ExploreQueryProcessor exploreQueryProcessor = new ExploreQueryProcessor( manager, authenticator ); // Explore-by-Example
            ExploreManager explore = ExploreManager.getInstance();
            explore.setExploreQueryProcessor( exploreQueryProcessor );

            HttpServer server = HttpServer.getInstance();
            server.addSerializedRoute( "/classifyData", explore::classifyData, HandlerType.POST );
            server.addSerializedRoute( "/getExploreTables", explore::getExploreTables, HandlerType.POST );
            server.addSerializedRoute( "/createInitialExploreQuery", explore::createInitialExploreQuery, HandlerType.POST );
            server.addSerializedRoute( "/exploration", explore::exploration, HandlerType.POST );
        }

    }


}
