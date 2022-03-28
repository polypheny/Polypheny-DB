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

package org.polypheny.db.adaptiveness.selfadaptiveness;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adaptiveness.exception.SelfAdaptiveRuntimeException;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.logical.LogicalTableScan;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.entity.MaterializedCriteria;
import org.polypheny.db.catalog.entity.MaterializedCriteria.CriteriaType;
import org.polypheny.db.catalog.exceptions.ColumnAlreadyExistsException;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.TableAlreadyExistsException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.exception.ColumnNotExistsException;
import org.polypheny.db.ddl.exception.LastPlacementException;
import org.polypheny.db.ddl.exception.PlacementAlreadyExistsException;
import org.polypheny.db.ddl.exception.PlacementNotExistsException;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.util.Pair;
import org.polypheny.db.view.SelfAdaptivAgent;

@Slf4j
public enum Action {

    /**
     * If the original Action was to add a Table, the Self-Adaptive Action is to move the table to a better suited store
     * therefore, first the new placement is added and after the worst old data placement is deleted.
     */
    SELECT_STORE_ADDITION {
        @Override
        public <T> void doChange( Decision decision, Transaction transaction ) {
            if(decision instanceof ManualDecision){
                ManualDecision manualDecision = ((ManualDecision)decision);

                if ( manualDecision.getPreSelection() != null ) {
                    // Do nothing, User selected something do not change that
                    return;
                }
                Catalog catalog = Catalog.getInstance();
                DdlManager ddlManager = DdlManager.getInstance();
                AdapterManager adapterManager = AdapterManager.getInstance();
                CatalogTable catalogTable = catalog.getTable( manualDecision.getEntityId() );
                DataStore bestDataStore = WeightedList.getBest( manualDecision.getWeightedList() );

                if ( !catalogTable.dataPlacements.contains( bestDataStore.getAdapterId() ) ) {

                    try {
                        ddlManager.addDataPlacement(
                                catalogTable,
                                catalogTable.columnIds,
                                Collections.emptyList(),
                                Collections.emptyList(),
                                bestDataStore,
                                transaction.createStatement() );
                    } catch ( PlacementAlreadyExistsException e ) {
                        log.warn( "Not possible to add this Placement because it already exists (Self-Adaptive-System)." );
                        throw new RuntimeException( "Not possible to add this Placement because it already exists (Self-Adaptive-System)." );
                    }

                    Pair<Object, Double> worstDataPlacement = null;
                    for ( Integer dataPlacement : catalogTable.dataPlacements ) {

                        DataStore dataStore = adapterManager.getStore( dataPlacement );
                        Double storeRanking = manualDecision.getWeightedList().get( dataStore );

                        if ( manualDecision.getWeightedList().containsKey( dataStore ) ) {
                            if ( worstDataPlacement == null ) {
                                worstDataPlacement = new Pair<>( dataStore, storeRanking );
                            } else if ( worstDataPlacement.right < storeRanking ) {
                                worstDataPlacement = new Pair<>( dataStore, storeRanking );
                            }
                        }
                    }
                    if ( worstDataPlacement != null ) {
                        try {
                            ddlManager.dropDataPlacement( catalogTable, (DataStore) worstDataPlacement.left, transaction.createStatement() );
                        } catch ( PlacementNotExistsException | LastPlacementException e ) {
                            log.warn( "Not possible to drop this Placement (Self-Adaptive-System)." );
                            throw new RuntimeException( "Not possible to drop this Placement (Self-Adaptive-System)." );
                        }
                    }
                }
            }
        }
    },
    SELECT_STORE_DELETION {
        @Override
        public <T> void doChange( Decision decision, Transaction transaction ) {

        }

    },
    MATERIALIZED_VIEW_ADDITION {
        @Override
        public <T> void doChange( Decision decision, Transaction transaction ) {
            if ( decision instanceof AutomaticDecision ){
                AutomaticDecision automaticDecision = (AutomaticDecision) decision;

                DdlManager ddlManager = DdlManager.getInstance();
                Statement statement = transaction.createStatement();
                AlgNode algNode = (AlgNode)automaticDecision.getSelected();

                try {
                    ddlManager.createMaterializedView( "materialized_" + decision.id ,
                            1,
                            AlgRoot.of(algNode, Kind.SELECT),
                            false,
                            statement,
                            null,
                            PlacementType.AUTOMATIC,
                            algNode.getRowType().getFieldNames(),
                            new MaterializedCriteria( CriteriaType.MANUAL ),
                            "",
                            Catalog.QueryLanguage.SQL,
                            false,
                            false
                            );
                } catch ( TableAlreadyExistsException | GenericCatalogException | UnknownColumnException | ColumnNotExistsException | ColumnAlreadyExistsException e ) {
                    throw new SelfAdaptiveRuntimeException( "Not Possible to add Materialized View (Self-Adaption)" );
                }

                // Create LogicalTableScan for Materialized View to replace it.
                AlgBuilder relBuilder = AlgBuilder.create( statement );
                final RexBuilder rexBuilder = relBuilder.getRexBuilder();
                final AlgOptCluster cluster = AlgOptCluster.create( statement.getQueryProcessor().getPlanner(), rexBuilder );
                PolyphenyDbCatalogReader reader = statement.getTransaction().getCatalogReader();
                AlgOptTable relOptTable = reader.getTable( Arrays.asList( "public", "materialized_" + decision.id ) );

                LogicalTableScan tableScan = LogicalTableScan.create( cluster, relOptTable  );


                SelfAdaptivAgentImpl.getInstance().addMaterializedViews(algNode.algCompareString(), tableScan);

            }
        }
    },
    MATERIALIZED_VIEW_DELETION {
        @Override
        public <T> void doChange( Decision decision, Transaction transaction ) {

        }
    },
    INDEX_ADDITION {
        @Override
        public <T> void doChange( Decision decision, Transaction transaction ) {
            if ( decision instanceof AutomaticDecision ){
                AutomaticDecision automaticDecision = (AutomaticDecision) decision;
                DdlManager ddlManager = DdlManager.getInstance();
                Statement statement = transaction.createStatement();
                AlgNode algNode = (AlgNode)automaticDecision.getSelected();

                List<String> test = algNode.getTable().getQualifiedName();
     

                log.warn( test.toString() );





            }

        }
    },
    INDEX_DELETION {
        @Override
        public <T> void doChange( Decision decision, Transaction transaction ) {

        }
    };


    public abstract <T> void doChange( Decision decision, Transaction transaction );


}
