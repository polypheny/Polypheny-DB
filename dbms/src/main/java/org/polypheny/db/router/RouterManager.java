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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.polypheny.db.config.Config;
import org.polypheny.db.config.Config.ConfigListener;
import org.polypheny.db.config.ConfigClazzList;
import org.polypheny.db.config.ConfigInteger;
import org.polypheny.db.config.ConfigManager;
import org.polypheny.db.config.WebUiGroup;
import org.polypheny.db.config.WebUiPage;
import org.polypheny.db.router.SimpleRouter.SimpleRouterFactory;
import org.polypheny.db.routing.Router;

@Slf4j
public class RouterManager {

    public static final ConfigInteger SHORT_RUNNING_LONG_RUNNING_THRESHOLD = new ConfigInteger(
            "routing/shortRunningLongRunningThreshold",
            "The minimal execution time (in milliseconds) for a query to be considered as long-running. Queries with lower execution times are considered as short-running.",
            1000 );

    public static final ConfigInteger PRE_COST_POST_COST_RATIO = new ConfigInteger(
            "routing/preCostPostCostRatio",
            "The ratio between how much post cost are considered. 0 means post cost are ignored, 1 means pre cost are ignored. Value most be between 0 and 1.",
            0 );

    private static final RouterManager INSTANCE = new RouterManager();

    private List<RouterFactory> shortRunningRouters;

    private List<RouterFactory> longRunningRouters;

    protected final WebUiPage routingPage;


    public static RouterManager getInstance() {
        return INSTANCE;
    }


    public RouterManager() {
        final ConfigManager configManager = ConfigManager.getInstance();
        routingPage = new WebUiPage(
                "routingPage",
                "Query Routing",
                "Settings influencing the query routing." );
        final WebUiGroup routingGroup = new WebUiGroup( "routingGroup", routingPage.getId(), 1 );
        routingGroup.withTitle( "General" );
        configManager.registerWebUiPage( routingPage );
        configManager.registerWebUiGroup( routingGroup );

        // Routing overall settings
        configManager.registerConfig( SHORT_RUNNING_LONG_RUNNING_THRESHOLD );
        SHORT_RUNNING_LONG_RUNNING_THRESHOLD.withUi( routingGroup.getId() );
        configManager.registerConfig( PRE_COST_POST_COST_RATIO );
        PRE_COST_POST_COST_RATIO.withUi( routingGroup.getId() );

        // Router settings
        final ConfigClazzList shortRunningRouter = new ConfigClazzList( "routing/shortRunningRouter", RouterFactory.class , true);
        configManager.registerConfig( shortRunningRouter );
        shortRunningRouter.withUi( routingGroup.getId() );
        shortRunningRouter.addObserver(getConfigListener(true));
        shortRunningRouters = getFactoryList( shortRunningRouter );

        final ConfigClazzList longRunningRouter = new ConfigClazzList( "routing/longRunningRouter", RouterFactory.class , true);
        configManager.registerConfig( longRunningRouter );
        longRunningRouter.withUi( routingGroup.getId() );
        longRunningRouter.addObserver(getConfigListener(false));
        longRunningRouters = getFactoryList( longRunningRouter );
    }

    public Router getSimpleRouter() {
        return new SimpleRouterFactory().createInstance();
    }

    public List<Router> getShortRunningRouters(){
        return shortRunningRouters.stream().map( elem -> elem.createInstance() ).collect( Collectors.toList());
    }

    public List<Router> getLongRunningRouters(){
        return shortRunningRouters.stream().map( elem -> elem.createInstance() ).collect( Collectors.toList());
    }

    private ConfigListener getConfigListener(boolean shortRunning){
        return new ConfigListener() {
            @Override
            public void onConfigChange( Config c ) {
                ConfigClazzList configClazzList = (ConfigClazzList) c;
                if(shortRunning){
                    shortRunningRouters = getFactoryList( configClazzList );
                }else{
                    longRunningRouters = getFactoryList( configClazzList );
                }

            }

            @Override
            public void restart( Config c ) {
            }
        };
    }

    private List<RouterFactory> getFactoryList( ConfigClazzList configList ) {
        val result = new ArrayList<RouterFactory>();
        for(Class c : configList.getClazzList()){
            try {
                Constructor<?> ctor = c.getConstructor();
                RouterFactory instance = (RouterFactory) ctor.newInstance();
                result.add( instance );
            } catch ( InstantiationException | InvocationTargetException | NoSuchMethodException | IllegalAccessException e ) {
                log.error( "Exception while changing router implementation", e );
            }
        }

        return result;

    }

}
