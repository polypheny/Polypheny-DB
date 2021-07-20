/*
 * Copyright 2019-2021 The Polypheny Project
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

import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.collections4.ListUtils;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.router.IcarusRouter.IcarusRouterFactory;
import org.polypheny.db.router.ReverseSimpleRouter.ReverseSimpleRouterFactory;
import org.polypheny.db.router.SimpleRouter.SimpleRouterFactory;
import org.polypheny.db.routing.ExecutionTimeMonitor;
import org.polypheny.db.routing.Router;
import org.polypheny.db.transaction.Statement;

@Slf4j
public class MultipleRouterRouting extends AbstractRouter{

    private final SimpleRouter simpleRouter;
    private final ReverseSimpleRouter reverseSimpleRouter;
    private final IcarusRouter icarusRouter;


    public MultipleRouterRouting( SimpleRouter simpleRouter, ReverseSimpleRouter reverseSimpleRouter, IcarusRouter icarusRouter ) {
        this.simpleRouter = simpleRouter;
        this.reverseSimpleRouter = reverseSimpleRouter;
        this.icarusRouter = icarusRouter;
    }


    @Override
    public List<DataStore> createTable( long schemaId, Statement statement ) {
        return this.simpleRouter.createTable( schemaId, statement );
    }


    @Override
    public List<DataStore> addColumn( CatalogTable catalogTable, Statement statement ) {
        return this.simpleRouter.addColumn( catalogTable, statement );
    }


    @Override
    public void dropPlacements( List<CatalogColumnPlacement> placements ) {
        return;
    }


    @Override
    protected void analyze( Statement statement, RelRoot logicalRoot ) {

    }

    @Override
    public List<RelRoot> route( RelRoot logicalRoot, Statement statement, ExecutionTimeMonitor executionTimeMonitor ) {
        boolean isAnalyze = statement.getTransaction().isAnalyze();
        if(isAnalyze){
            statement.getTransaction().deactivateAnalyze();
        }
        val simpleRoutingRels = this.simpleRouter.route( logicalRoot, statement, executionTimeMonitor );
        val reverseSimpleRoutingRels = this.reverseSimpleRouter.route( logicalRoot, statement, executionTimeMonitor );
        List<RelRoot> icarusRoutingRels = new ArrayList<>();
        try{
            icarusRoutingRels = this.icarusRouter.route( logicalRoot, statement, executionTimeMonitor );
        }catch ( Exception e){
           // ignore icarus routing when no full table placement is available
        }


        if(isAnalyze){
            statement.getTransaction().activateAnalyze();
        }

        return ListUtils.union( ListUtils.union( simpleRoutingRels, reverseSimpleRoutingRels), icarusRoutingRels);
    }


    @Override
    protected void wrapUp( Statement statement, RelNode routed ) {

    }


    @Override
    protected List<CatalogColumnPlacement> selectPlacement( RelNode node, CatalogTable catalogTable ) {
        return null;
    }

    public static class MultipleRouterRoutingFactory extends RouterFactory{

        @Override
        public Router createInstance() {
            return new MultipleRouterRouting(
                    SimpleRouterFactory.createSimpleRouterInstance(),
                    ReverseSimpleRouterFactory.createReverseSimpleRouterInstance(),
                    IcarusRouterFactory.creatIcarusRouterInstance() );
        }

    }

}
