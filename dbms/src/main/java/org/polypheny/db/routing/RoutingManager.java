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

package org.polypheny.db.routing;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.config.Config;
import org.polypheny.db.config.Config.ConfigListener;
import org.polypheny.db.config.ConfigBoolean;
import org.polypheny.db.config.ConfigClazz;
import org.polypheny.db.config.ConfigClazzList;
import org.polypheny.db.config.ConfigDouble;
import org.polypheny.db.config.ConfigEnum;
import org.polypheny.db.config.ConfigManager;
import org.polypheny.db.config.WebUiGroup;
import org.polypheny.db.routing.factories.RouterFactory;
import org.polypheny.db.routing.routers.CachedPlanRouter;
import org.polypheny.db.routing.routers.DmlRouterImpl;
import org.polypheny.db.routing.routers.SimpleRouter;
import org.polypheny.db.routing.routers.SimpleRouter.SimpleRouterFactory;
import org.polypheny.db.routing.strategies.CreatePlacementStrategy;
import org.polypheny.db.routing.strategies.CreateSinglePlacementStrategy;
import org.polypheny.db.routing.strategies.RoutingPlanSelector;


@Slf4j
public class RoutingManager {

    public static final ConfigDouble PRE_COST_POST_COST_RATIO = new ConfigDouble(
            "routing/preCostPostCostRatio",
            "The ratio between how much post cost are considered. 0 means post cost are ignored, 1 means pre cost are ignored. Value must be between 0 and 1.",
            0 );

    public static final ConfigBoolean POST_COST_AGGREGATION_ACTIVE = new ConfigBoolean(
            "routing/postCostAggregationActive",
            "Determines whether the post cost aggregation is active or not. If active, system should be in single thread mode, otherwise costs are hard to compare.",
            true );

    public static final ConfigEnum PLAN_SELECTION_STRATEGY = new ConfigEnum(
            "routing/planSelectionStrategy",
            "Defines whether the best plan will be returned or the plan based on percentage calculated for each plan ordered by costs",
            RouterPlanSelectionStrategy.class,
            RouterPlanSelectionStrategy.BEST );


    private static final RoutingManager INSTANCE = new RoutingManager();

    @Getter
    private final DmlRouter dmlRouter = new DmlRouterImpl();
    @Getter
    private final CachedPlanRouter cachedPlanRouter = new CachedPlanRouter();
    @Getter
    private final SimpleRouter fallbackRouter = (SimpleRouter) new SimpleRouterFactory().createInstance();
    @Getter
    private final RoutingPlanSelector routingPlanSelector = new RoutingPlanSelector();
    @Getter
    private CreatePlacementStrategy createPlacementStrategy = new CreateSinglePlacementStrategy();
    private List<RouterFactory> routerFactories;


    public RoutingManager() {
        this.initializeConfigUi();
    }


    public static RoutingManager getInstance() {
        return INSTANCE;
    }


    public List<Router> getRouters() {
        return routerFactories.stream()
                .map( RouterFactory::createInstance )
                .toList();
    }


    public void initializeConfigUi() {
        final ConfigManager configManager = ConfigManager.getInstance();
        final WebUiGroup routingGroup = new WebUiGroup( "routingGeneral", "routing" );
        routingGroup.withTitle( "General" );
        configManager.registerWebUiGroup( routingGroup );

        // Settings
        final ConfigClazz tablePlacementStrategy = new ConfigClazz(
                "routing/createPlacementStrategy",
                CreatePlacementStrategy.class,
                CreateSinglePlacementStrategy.class );
        configManager.registerConfig( tablePlacementStrategy );
        tablePlacementStrategy.withUi( routingGroup.getId() );
        tablePlacementStrategy.addObserver( new ConfigListener() {
            @Override
            public void onConfigChange( Config c ) {
                ConfigClazz configClazz = (ConfigClazz) c;
                if ( tablePlacementStrategy.getClass() != configClazz.getClazz() ) {
                    setCreatePlacementStrategy( configClazz );
                }
            }


            @Override
            public void restart( Config c ) {
            }
        } );

        // Router settings
        final ConfigClazzList routerClassesConfig = new ConfigClazzList(
                "routing/routers",
                RouterFactory.class,
                true );
        configManager.registerConfig( routerClassesConfig );
        routerClassesConfig.withUi( routingGroup.getId(), 0 );
        routerClassesConfig.addObserver( getRouterConfigListener() );
        routerFactories = getFactoryList( routerClassesConfig );

        configManager.registerConfig( PRE_COST_POST_COST_RATIO );
        PRE_COST_POST_COST_RATIO.withUi( routingGroup.getId(), 1 );

        configManager.registerConfig( PLAN_SELECTION_STRATEGY );
        PLAN_SELECTION_STRATEGY.withUi( routingGroup.getId(), 2 );

        configManager.registerConfig( POST_COST_AGGREGATION_ACTIVE );
        POST_COST_AGGREGATION_ACTIVE.addObserver( new ConfigListener() {
            @Override
            public void onConfigChange( Config c ) {
                String status = c.getBoolean() ? "Enabled" : "Disabled";
                log.warn( "{} post cost aggregation", status );
            }


            @Override
            public void restart( Config c ) {

            }
        } );
        POST_COST_AGGREGATION_ACTIVE.withUi( routingGroup.getId(), 3 );
    }


    private void setCreatePlacementStrategy( ConfigClazz implementation ) {
        try {
            Constructor<?> ctor = implementation.getClazz().getConstructor();
            this.createPlacementStrategy = (CreatePlacementStrategy) ctor.newInstance();
        } catch ( InstantiationException | InvocationTargetException | NoSuchMethodException | IllegalAccessException e ) {
            log.error( "Exception while changing table placement strategy", e );
        }
    }


    private ConfigListener getRouterConfigListener() {
        return new ConfigListener() {
            @Override
            public void onConfigChange( Config c ) {
                ConfigClazzList configClazzList = (ConfigClazzList) c;
                routerFactories = getFactoryList( configClazzList );
            }


            @Override
            public void restart( Config c ) {
            }
        };
    }


    private List<RouterFactory> getFactoryList( ConfigClazzList configList ) {
        final List<RouterFactory> result = new ArrayList<>();
        for ( Class<? extends RouterFactory> c : configList.getClazzList() ) {
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
