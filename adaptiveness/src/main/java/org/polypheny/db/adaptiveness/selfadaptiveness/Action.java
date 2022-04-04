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

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adaptiveness.JoinInformation;
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
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownKeyException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.exception.AlterSourceException;
import org.polypheny.db.ddl.exception.ColumnNotExistsException;
import org.polypheny.db.ddl.exception.IndexExistsException;
import org.polypheny.db.ddl.exception.LastPlacementException;
import org.polypheny.db.ddl.exception.MissingColumnPlacementException;
import org.polypheny.db.ddl.exception.PlacementAlreadyExistsException;
import org.polypheny.db.ddl.exception.PlacementNotExistsException;
import org.polypheny.db.ddl.exception.UnknownIndexMethodException;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.util.Pair;

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
                Catalog catalog = Catalog.getInstance();
                Statement statement = transaction.createStatement();

                JoinInformation joinInfo = automaticDecision.getWorkloadInformation().getJoinInformation();
                if(joinInfo != null){
                    for ( Pair<Long, Long> jointTableId : joinInfo.getJointTableIds() ) {
                        CatalogTable catalogTableLeft = catalog.getTable( jointTableId.left);
                        CatalogTable catalogTableRight = catalog.getTable( jointTableId.right );


                        // Select best store not random store
                        List<DataStore> possibleStores;
                        Map<String, DataStore> availableStores = AdapterManager.getInstance().getStores();
                        possibleStores = new ArrayList<>( availableStores.values() );


                        try {
                            // All cols
                            /*
                            ddlManager.addIndex( catalogTableLeft, null, catalogTableLeft.getColumnNames(), "selfAdaptiveIndex_"+ + decision.id , false, null, statement );
                            ddlManager.addIndex( catalogTableRight, null, catalogTableRight.getColumnNames(), "selfAdaptiveIndex_"+ + decision.id , false, null, statement );
                             */


                            // Primarykey cols
                            List<String> colNamesPrimaryLeft = new ArrayList<>();
                            catalog.getPrimaryKey( catalogTableLeft.primaryKey ).columnIds.forEach( id -> colNamesPrimaryLeft.add( catalog.getColumn( id ).name ) );
                            ddlManager.addIndex( catalogTableLeft, null, colNamesPrimaryLeft, "selfAdaptiveIndex_"+ + decision.id , false, possibleStores.get( 1 ), statement );

                            List<String> colNamesPrimaryRight= new ArrayList<>();
                            catalog.getPrimaryKey( catalogTableLeft.primaryKey ).columnIds.forEach( id -> colNamesPrimaryRight.add( catalog.getColumn( id ).name ) );
                            ddlManager.addIndex( catalogTableRight, null, colNamesPrimaryRight, "selfAdaptiveIndex_"+ + decision.id , false, possibleStores.get( 1 ), statement );



                        } catch ( UnknownColumnException | UnknownIndexMethodException | GenericCatalogException | UnknownTableException | UnknownUserException | UnknownSchemaException | UnknownKeyException | UnknownDatabaseException | TransactionException | AlterSourceException | IndexExistsException | MissingColumnPlacementException e ) {
                            e.printStackTrace();
                        }
                    }
                }

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
