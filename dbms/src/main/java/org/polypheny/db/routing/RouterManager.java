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

package org.polypheny.db.routing;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.polypheny.db.config.Config;
import org.polypheny.db.config.Config.ConfigListener;
import org.polypheny.db.config.ConfigBoolean;
import org.polypheny.db.config.ConfigClazz;
import org.polypheny.db.config.ConfigClazzList;
import org.polypheny.db.config.ConfigDouble;
import org.polypheny.db.config.ConfigEnum;
import org.polypheny.db.config.ConfigInteger;
import org.polypheny.db.config.ConfigManager;
import org.polypheny.db.config.WebUiGroup;
import org.polypheny.db.config.WebUiPage;
import org.polypheny.db.routing.factories.RouterFactory;
import org.polypheny.db.routing.routers.DmlRouterImpl;
import org.polypheny.db.routing.routers.SimpleRouter;
import org.polypheny.db.routing.routers.SimpleRouter.SimpleRouterFactory;
import org.polypheny.db.routing.tablePlacements.SingleTablePlacementStrategy;
import org.polypheny.db.routing.tablePlacements.TablePlacementStrategy;

@Slf4j
public class RouterManager {

    public static final ConfigInteger SHORT_RUNNING_LONG_RUNNING_THRESHOLD = new ConfigInteger(
            "routing/shortRunningLongRunningThreshold",
            "The minimal execution time (in milliseconds) for a query to be considered as long-running. Queries with lower execution times are considered as short-running.",
            1000 );

    public static final ConfigDouble PRE_COST_POST_COST_RATIO = new ConfigDouble(
            "routing/preCostPostCostRatio",
            "The ratio between how much post cost are considered. 0 means post cost are ignored, 1 means pre cost are ignored. Value most be between 0 and 1.",
            0 );

    public static final ConfigBoolean IS_LONG_ACTIVE = new ConfigBoolean(
            "routing/distinctionBetweenShortAndLongRunning",
            "Boolean whether to distinguish between long and short running queries. If set to false, the following configuration will not be considered.",
            false );

    public static final ConfigEnum PLAN_SELECTION_STRATEGY = new ConfigEnum(
            "routing/planSelectionStrategy",
            "Defines whether the best plan will be returned or the plan based on percentage calculated for each plan orderd by costs",
            RouterPlanSelectionStrategy.class, RouterPlanSelectionStrategy.BEST);


    private static final RouterManager INSTANCE = new RouterManager();
    protected final WebUiPage routingPage;
    private List<RouterFactory> shortRunningRouters;
    private Optional<List<RouterFactory>> longRunningRouters = Optional.empty();
    private TablePlacementStrategy tablePlacementStrategy = new SingleTablePlacementStrategy();

    @Getter
    private final DmlRouter dmlRouter = new DmlRouterImpl();
    @Getter
    private final CachedPlanRouter cachedPlanRouter = new CachedPlanRouter();
    @Getter
    private final SimpleRouter fallbackRouter = (SimpleRouter) new SimpleRouterFactory().createInstance();



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

        // Settings
        final ConfigClazz tablePlacementStrategy = new ConfigClazz( "routing/tablePlacementStrategy", TablePlacementStrategy.class, SingleTablePlacementStrategy.class );
        configManager.registerConfig( tablePlacementStrategy );
        tablePlacementStrategy.withUi( routingGroup.getId() );
        tablePlacementStrategy.addObserver( new ConfigListener() {
            @Override
            public void onConfigChange( Config c ) {
                ConfigClazz configClazz = (ConfigClazz) c;
                if ( tablePlacementStrategy.getClass() != configClazz.getClazz() ) {
                    log.warn( "Change router implementation: " + configClazz.getClazz() );
                    setTablePlacementStrategy( configClazz );
                }
            }


            @Override
            public void restart( Config c ) {
            }
        } );

        // Router settings
        final ConfigClazzList shortRunningRouter = new ConfigClazzList( "routing/routers", RouterFactory.class, true );
        configManager.registerConfig( shortRunningRouter );
        shortRunningRouter.withUi( routingGroup.getId(), 0 );
        shortRunningRouter.addObserver( getConfigListener( true ) );
        shortRunningRouters = getFactoryList( shortRunningRouter );

        configManager.registerConfig( PRE_COST_POST_COST_RATIO );
        PRE_COST_POST_COST_RATIO.withUi( routingGroup.getId(), 1 );

        configManager.registerConfig( PLAN_SELECTION_STRATEGY );
        PLAN_SELECTION_STRATEGY.withUi( routingGroup.getId(), 2 );

        configManager.registerConfig( IS_LONG_ACTIVE );
        IS_LONG_ACTIVE.withUi( routingGroup.getId(), 3 );
        IS_LONG_ACTIVE.addObserver( getLongRunningLister( routingGroup ) );
    }


    public static RouterManager getInstance() {
        return INSTANCE;
    }


    private ConfigListener getLongRunningLister( WebUiGroup routingGroup ) {
        return new ConfigListener() {
            @Override
            public void onConfigChange( Config c ) {
                ConfigBoolean bool = (ConfigBoolean) c;

                final ConfigManager configManager = ConfigManager.getInstance();

                if ( c.getBoolean() ) {
                    // long running activate, make it configurable:
                    // Routing overall settings
                    configManager.registerConfig( SHORT_RUNNING_LONG_RUNNING_THRESHOLD );
                    SHORT_RUNNING_LONG_RUNNING_THRESHOLD.withUi( routingGroup.getId(), 4 );

                    final ConfigClazzList longRunningRouter = new ConfigClazzList( "routing/longRunningRouter", RouterFactory.class, true );
                    configManager.registerConfig( longRunningRouter );
                    longRunningRouter.withUi( routingGroup.getId(), 5 );
                    longRunningRouter.addObserver( getConfigListener( false ) );
                    longRunningRouters = Optional.of( getFactoryList( longRunningRouter ) );
                } else {
                    // todo: remove it again
                    // not supported yet?
                }


            }


            @Override
            public void restart( Config c ) {

            }
        };
    }


    public TablePlacementStrategy getTablePlacementStrategy() {
        return this.tablePlacementStrategy;
    }


    private void setTablePlacementStrategy( ConfigClazz implementation ) {
        try {
            Constructor<?> ctor = implementation.getClazz().getConstructor();
            TablePlacementStrategy instance = (TablePlacementStrategy) ctor.newInstance();
            this.tablePlacementStrategy = instance;
        } catch ( InstantiationException | InvocationTargetException | NoSuchMethodException | IllegalAccessException e ) {
            log.error( "Exception while changing table placement strategy", e );
        }
    }


    public List<Router> getShortRunningRouters() {
        return shortRunningRouters.stream().map( elem -> elem.createInstance() ).collect( Collectors.toList() );
    }


    public List<Router> getLongRunningRouters() {
        if ( longRunningRouters.isPresent() ) {
            return longRunningRouters.get().stream().map( elem -> elem.createInstance() ).collect( Collectors.toList() );
        }
        return Collections.emptyList();
    }


    private ConfigListener getConfigListener( boolean shortRunning ) {
        return new ConfigListener() {
            @Override
            public void onConfigChange( Config c ) {
                ConfigClazzList configClazzList = (ConfigClazzList) c;
                if ( shortRunning ) {
                    shortRunningRouters = getFactoryList( configClazzList );
                } else {
                    longRunningRouters = Optional.of( getFactoryList( configClazzList ) );
                }

            }


            @Override
            public void restart( Config c ) {
            }
        };
    }


    private List<RouterFactory> getFactoryList( ConfigClazzList configList ) {
        val result = new ArrayList<RouterFactory>();
        for ( Class c : configList.getClazzList() ) {
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
