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

package org.polypheny.db.policies.policy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.policies.policy.Clause.ClauseName;
import org.polypheny.db.policies.policy.Policy.Target;
import org.polypheny.db.policies.policy.exception.PolicyRuntimeException;
import org.polypheny.db.policies.policy.models.Policies;
import org.polypheny.db.policies.policy.models.PolicyChangedRequest;

@Slf4j
public class PolicyManager {

    private static PolicyManager INSTANCE = null;


    public static PolicyManager getInstance() {
        if ( INSTANCE == null ) {
            INSTANCE = new PolicyManager();
        }
        return INSTANCE;
    }


    // id -> Policy
    private final Map<Integer, Policy> policies = new HashMap<>();

    //polyphenyPolicy -> id
    private int polyphenyPolicyId;

    // map <schemaId -> policyId>
    private final Map<Long, Integer> namespacePolicies = new HashMap<>();

    // map <entityId -> policyId>
    private final Map<Long, Integer> entityPolicies = new HashMap<>();


    /**
     * Initialize all policies, create policy for Polypheny, each namespace and entities and registers all possible clauses.
     */
    public void initialize() {
        if ( RuntimeConfig.POLICY.getBoolean() ) {
            ClausesRegister.registerClauses();
            createDefaultPolicies();

        }
    }


    /**
     * Creates policy for Polypheny, all namespaces and entities.
     */
    private void createDefaultPolicies() {
        Catalog catalog = Catalog.getInstance();

        // Policy for Polypheny
        Policy polyphenyPolicy = new Policy();
        polyphenyPolicyId = polyphenyPolicy.getId();
        policies.put( polyphenyPolicy.getId(), polyphenyPolicy );

        // Policy for each namespace
        List<CatalogSchema> namespaces = catalog.getSchemas( 1, null );
        for ( CatalogSchema namespace : namespaces ) {
            Policy schemaPolicy = new Policy( Target.NAMESPACE, namespace.id );
            namespacePolicies.put( namespace.id, schemaPolicy.getId() );
            policies.put( schemaPolicy.getId(), schemaPolicy );
            // Policy for each entity
            List<CatalogTable> entities = catalog.getTables( namespace.id, null );
            for ( CatalogTable entity : entities ) {
                Policy tablePolicy = new Policy( Target.ENTITY, entity.id );
                entityPolicies.put( entity.id, tablePolicy.getId() );
                policies.put( tablePolicy.getId(), tablePolicy );
            }
        }
    }


    public Object getPolicies( String polypheny, Long namespaceId, Long entityId ) throws RuntimeException {
        List<Policies> policies = new ArrayList<>();
        // for the whole system
        if ( polypheny != null ) {
            Policy policy = this.policies.get( polyphenyPolicyId );
            getRelevantClauses( policies, policy );
        }
        // for a namespace
        else if ( entityId == null ) {
            Policy policy = this.policies.get( namespacePolicies.get( namespaceId ) );
            getRelevantClauses( policies, policy );
        }
        // for a entity
        else {
            Policy policy = this.policies.get( entityPolicies.get( entityId ) );
            getRelevantClauses( policies, policy );
        }
        if ( policies.isEmpty() ) {
            throw new PolicyRuntimeException( "There are no policies for this target." );
        } else {
            return policies;
        }
    }


    private void getRelevantClauses( List<Policies> policies, Policy policy ) {
        if ( !policy.getClauses().isEmpty() ) {
            for ( Clause clause : policy.getClauses().values() ) {
                policies.add( new Policies( clause.getClauseName().name(), policy.getTarget(), clause, clause.getClauseType(), clause.getDescription()) );
            }
        }
    }



    public void getPossiblePolicies( String polypheny, Long schemaId, Long tableId ) {
/*
        AdapterManager.getInstance().getAdapters().forEach( (k, v) ->
                v.getDeployMode());
                }
        );

 */
    }

    public void updatePolicies( PolicyChangedRequest changeRequest ) {

        for ( Policy policy : policies.values() ) {
            if ( !policy.getClauses().isEmpty() ) {
                if ( policy.getTarget().name().equals( changeRequest.targetName ) ) {
                    if ( changeRequest.requestType.equals( "BooleanChangeRequest" ) ) {
                        BooleanClause clause = (BooleanClause) policy.getClauses().get( changeRequest.clauseId );
                        clause.setValue( changeRequest.booleanValue );
                        if ( checkClauseChange( clause, policy.getTarget(), policy.getTargetId() ) ) {
                            policy.getClauses().put( changeRequest.clauseId, clause );
                            log.warn( "Persistency should change: " + clause.isValue() );
                        } else {
                            log.warn( "Persistency not possible to change: " + clause.isValue() );
                            throw new PolicyRuntimeException( "Not possible to change this clause because the policies can not be guaranteed anymore." );
                        }


                    }
                }
            }
        }


    }


    /**
     * Check if it is possible to switch on a policy, if something is against a policy it is not possible to change it.
     * At the moment only Stores are checked.
     *
     * @param clause that was changed
     * @param target of the changed clause
     * @return true if it is possible to change clause otherwise false
     */
    private boolean checkClauseChange( BooleanClause clause, Target target, Long targetId ) {
        switch ( clause.getClauseName() ) {
            case FULLY_PERSISTENT:
                List<Integer> storesToCheck = new ArrayList<>();
                AdapterManager.getInstance().getStores().forEach( ( s, dataStore ) -> {
                            if ( !dataStore.isPersistent() ) {
                                storesToCheck.add( dataStore.getAdapterId() );
                            }
                        }
                );

                List<CatalogPartitionPlacement> partitionPlacements = Catalog.getInstance().getAllPartitionPlacement();

                switch ( target ) {
                    case POLYPHENY:
                        for ( CatalogPartitionPlacement partitionPlacement : partitionPlacements ) {
                            if ( storesToCheck.contains( partitionPlacement.adapterId ) ) {
                                return false;
                            }
                        }
                        break;
                    case NAMESPACE:
                        for ( CatalogPartitionPlacement partitionPlacement : partitionPlacements ) {
                            if ( storesToCheck.contains( partitionPlacement.adapterId ) && partitionPlacement.schemaId == targetId ) {
                                return false;
                            }
                        }
                        break;
                    case ENTITY:
                        for ( CatalogPartitionPlacement partitionPlacement : partitionPlacements ) {
                            if ( storesToCheck.contains( partitionPlacement.adapterId ) && partitionPlacement.tableId == targetId ) {
                                return false;
                            }
                        }
                        break;
                }
                break;
            default:
                throw new PolicyRuntimeException( "Check if it is possible to change clause is not implemented yet." );
        }
        return true;
    }


    public <T> List<T> makeDecision( Class<T> clazz, Action action, Long namespaceId, Long entityId ) {
        return makeDecision( clazz, action, namespaceId, entityId, null );
    }


    public <T> List<T> makeDecision( Class<T> clazz, Action action, Long namespaceId, Long entityId, T preSelection ) {
        List<Clause> potentiallyInteresting = new ArrayList<>();

        switch ( action ) {
            case CREATE_TABLE:
                List<Integer> possibleStoreIds = new ArrayList<>();
                // Get all availableStores
                Map<String, DataStore> availableStores = AdapterManager.getInstance().getStores();

                // Check if there are relevant policies and add it to potentiallyInteresting
                potentiallyInteresting.add( checkClauses( null, ClauseName.FULLY_PERSISTENT, Target.POLYPHENY ) );
                potentiallyInteresting.add( checkClauses( namespaceId, ClauseName.FULLY_PERSISTENT, Target.NAMESPACE ) );
                potentiallyInteresting.add( checkClauses( entityId, ClauseName.FULLY_PERSISTENT, Target.ENTITY ) );

                for ( Clause clause : potentiallyInteresting ) {
                    if ( clause != null ) {
                        if ( ((BooleanClause) clause).isValue() ) {
                            for ( DataStore store : availableStores.values() ) {
                                if ( store.isPersistent() ) {
                                    possibleStoreIds.add( store.getAdapterId() );
                                }
                            }
                        } else {
                            for ( DataStore store : availableStores.values() ) {
                                possibleStoreIds.add( store.getAdapterId() );
                            }
                        }
                    }
                }

                return (List<T>) possibleStoreIds;

            case ADD_PLACEMENT:

                // Check if there are relevant policies and add it to potentiallyInteresting
                potentiallyInteresting.add( checkClauses( null, ClauseName.FULLY_PERSISTENT, Target.POLYPHENY ) );
                potentiallyInteresting.add( checkClauses( namespaceId, ClauseName.FULLY_PERSISTENT, Target.NAMESPACE ) );
                potentiallyInteresting.add( checkClauses( entityId, ClauseName.FULLY_PERSISTENT, Target.ENTITY ) );

                for ( Clause clause : potentiallyInteresting ) {
                    if ( clause != null ) {
                        if ( ((BooleanClause) clause).isValue() ) {
                            DataStore dataStore = AdapterManager.getInstance().getStore( (int) preSelection );
                            if ( dataStore.isPersistent() ) {
                                return Collections.singletonList( preSelection );
                            } else {
                                return Collections.emptyList();
                            }
                        } else {
                            return Collections.singletonList( preSelection );
                        }
                    }
                }

            case ADD_PARTITIONING:
                List<DataStore> possibleStores = new ArrayList<>();

                // Check if there are relevant policies and add it to potentiallyInteresting
                potentiallyInteresting.add( checkClauses( null, ClauseName.FULLY_PERSISTENT, Target.POLYPHENY ) );
                potentiallyInteresting.add( checkClauses( namespaceId, ClauseName.FULLY_PERSISTENT, Target.NAMESPACE ) );
                potentiallyInteresting.add( checkClauses( entityId, ClauseName.FULLY_PERSISTENT, Target.ENTITY ) );

                for ( Clause clause : potentiallyInteresting ) {
                    if ( clause != null ) {
                        if ( ((BooleanClause) clause).isValue() ) {
                            for ( DataStore store : (List<DataStore>) preSelection ) {
                                if ( store.isPersistent() ) {
                                    possibleStores.add( store );
                                }
                            }
                        } else {
                            possibleStores.addAll( (List<DataStore>) preSelection );
                        }
                    }
                }
                return (List<T>) possibleStores;
            default:
                throw new PolicyRuntimeException( "Not implemented action was used to make a Decision" );
        }

    }


    private Clause checkClauses( Long targetId, ClauseName clauseName, Target target ) {
        switch ( target ) {
            case POLYPHENY:
                return getRelevantClauses( polyphenyPolicyId, clauseName );
            case NAMESPACE:
                if ( namespacePolicies.containsKey( targetId ) ) {
                    Integer policyId = namespacePolicies.get( targetId );
                    if ( policies.get( policyId ).getClausesByName().containsKey( clauseName ) ) {
                        return getRelevantClauses( policyId, clauseName );
                    }
                }
                return null;
            case ENTITY:
                if ( entityPolicies.containsKey( targetId ) ) {
                    Integer policyId = entityPolicies.get( targetId );
                    if ( policies.get( policyId ).getClausesByName().containsKey( clauseName ) ) {
                        return getRelevantClauses( policyId, clauseName );
                    }
                }
                return null;
            default:
                throw new PolicyRuntimeException( "For " + target.name() + " is the clause check not implemented" );
        }
    }


    private Clause getRelevantClauses( Integer policyId, ClauseName clauseName ) {
        Integer clauseId = policies.get( policyId ).getClausesByName().get( clauseName );
        return policies.get( policyId ).getClauses().get( clauseId );
    }


    public enum Action {
        CREATE_TABLE, ADD_PARTITIONING, ADD_PLACEMENT

    }

}
