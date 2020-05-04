/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.router;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.config.Config;
import org.polypheny.db.config.Config.ConfigListener;
import org.polypheny.db.config.ConfigClazz;
import org.polypheny.db.config.ConfigManager;
import org.polypheny.db.config.WebUiGroup;
import org.polypheny.db.config.WebUiPage;
import org.polypheny.db.router.IcarusRouter.IcarusRouterFactory;
import org.polypheny.db.router.SimpleRouter.SimpleRouterFactory;
import org.polypheny.db.routing.Router;

@Slf4j
public class RouterManager {

    private static final RouterManager INSTANCE = new RouterManager();

    private RouterFactory currentRouter = null;


    public static RouterManager getInstance() {
        return INSTANCE;
    }


    public RouterManager() {
        setCurrentRouter( new IcarusRouterFactory() );

        final ConfigManager configManager = ConfigManager.getInstance();
        final WebUiPage routingPage = new WebUiPage(
                "routingPage",
                "Routing Settings",
                "Settings influencing the query routing." );
        final WebUiGroup routingGroup = new WebUiGroup( "routingGroup", routingPage.getId() );
        configManager.registerWebUiPage( routingPage );
        configManager.registerWebUiGroup( routingGroup );

        // Settings
        final ConfigClazz routerImplementation = new ConfigClazz( "routing/router", RouterFactory.class, SimpleRouterFactory.class );
        configManager.registerConfig( routerImplementation );
        routerImplementation.withUi( routingGroup.getId() );
        routerImplementation.addObserver( new ConfigListener() {
            @Override
            public void onConfigChange( Config c ) {
                ConfigClazz configClazz = (ConfigClazz) c;
                if ( currentRouter.getClass() != configClazz.getClazz() ) {
                    log.warn( "Change router implementation: " + configClazz.getClazz() );
                    try {
                        Constructor<?> ctor = configClazz.getClazz().getConstructor();
                        RouterFactory instance = (RouterFactory) ctor.newInstance();
                        setCurrentRouter( instance );
                    } catch ( InstantiationException | InvocationTargetException | NoSuchMethodException | IllegalAccessException e ) {
                        log.error( "Exception while changing router implementation", e );
                    }
                }
            }

            @Override
            public void restart( Config c ) {
            }
        } );
    }


    public void setCurrentRouter( RouterFactory routerFactory ) {
        this.currentRouter = routerFactory;
    }


    public Router getRouter() {
        return currentRouter.createInstance();
    }

}
