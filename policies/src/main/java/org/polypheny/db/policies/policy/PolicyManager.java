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

import static org.polypheny.db.policies.policy.firstDraft.PolicyFirstDraft.TARGET_POLYPHENY;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.policies.policy.Clause.ClauseName;
import org.polypheny.db.policies.policy.Policy.Target;
import org.polypheny.db.policies.policy.exception.PolicyRuntimeException;
import org.polypheny.db.policies.policy.models.PolicyChangeRequest;
import org.polypheny.db.policies.policy.models.UiPolicy;
import org.polypheny.db.util.Pair;

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


    /**
     * Finds the target if a UI-Request was used and the target is not known.
     */
    public Pair<Target, Long> findTarget( String polypheny, Long namespaceId, Long entityId ) {
        if ( polypheny != null ) {
            return new Pair<>( Target.POLYPHENY, TARGET_POLYPHENY );
        }
        // for a namespace
        else if ( namespaceId != null && entityId == null ) {
            return new Pair<>( Target.NAMESPACE, namespaceId );
        }
        // for an entity
        else if ( entityId != null ) {
            return new Pair<>( Target.ENTITY, entityId );
        } else {
            throw new PolicyRuntimeException( "Not possible to find target. There is no target that matches." );
        }
    }


    /**
     * This method returns all active policies for a specific target.
     */
    public List<UiPolicy> getPolicies( Pair<Target, Long> target ) {
        List<UiPolicy> targetPolicies = new ArrayList<>();

        switch ( target.left ) {
            case POLYPHENY:
                targetPolicies.addAll( getRelevantClauses( polyphenyPolicyId ) );
                break;
            case NAMESPACE:
                targetPolicies.addAll( getRelevantClauses( namespacePolicies.get( target.right ) ) );
                break;
            case ENTITY:
                targetPolicies.addAll( getRelevantClauses( entityPolicies.get( target.right ) ) );
                break;
            default:
                log.warn( "This Target is not implemented yet." );
                throw new PolicyRuntimeException( "This Target is not implemented yet." );
        }
        return targetPolicies;
    }


    private List<UiPolicy> getRelevantClauses( int policyId ) {
        List<UiPolicy> policies = new ArrayList<>();
        Policy policy = this.policies.get( policyId );
        if ( !policy.getClauses().isEmpty() ) {
            for ( Clause clause : policy.getClauses().values() ) {
                policies.add( new UiPolicy( clause.getClauseName().name(), policy.getTarget(), policy.getTargetId(), clause, clause.getClauseType(), clause.getDescription() ) );
            }
        }
        return policies;
    }


    /**
     * Checks all the registered clauses for a specific target and returns it.
     */
    public List<UiPolicy> getPossiblePolicies( Pair<Target, Long> target ) {
        List<UiPolicy> targetPolicies = new ArrayList<>();
        long targetId = target.right;
        switch ( target.left ) {
            case POLYPHENY:
                targetPolicies.addAll( getRelevantPolicies( polyphenyPolicyId, targetId, target.left ) );
                break;
            case NAMESPACE:
                targetPolicies.addAll( getRelevantPolicies( namespacePolicies.get( targetId ), targetId, target.left ) );
                break;
            case ENTITY:
                targetPolicies.addAll( getRelevantPolicies( entityPolicies.get( targetId ), targetId, target.left ) );
                break;
            default:
                log.warn( "This Target is not implemented yet." );
                throw new PolicyRuntimeException( "This Target is not implemented yet." );
        }
        return targetPolicies;
    }


    private List<UiPolicy> getRelevantPolicies( int policyId, long targetId, Target target ) {
        Map<ClauseName, Clause> registeredClauses = ClausesRegister.getRegistry();
        List<UiPolicy> relevantPolicies = new ArrayList<>();
        for ( Clause clause : registeredClauses.values() ) {
            if ( clause.getPossibleTargets().contains( target ) && !this.policies.get( policyId ).getClauses().containsKey( clause.getClauseName() ) ) {
                relevantPolicies.add( new UiPolicy( clause.getClauseName().name(), target, targetId, clause, clause.getClauseType(), clause.getDescription() ) );
            }
        }
        return relevantPolicies;
    }


    public void updatePolicies( PolicyChangeRequest changeRequest ) {

        Target target = Target.valueOf( changeRequest.targetName );

        for ( Policy policy : policies.values() ) {
            if ( !policy.getClauses().isEmpty() ) {
                if ( policy.getTarget() == target ) {
                    if ( changeRequest.requestType.equals( "BooleanChangeRequest" ) ) {
                        BooleanClause clause = (BooleanClause) policy.getClauses().get( ClauseName.valueOf( changeRequest.clauseName ) );
                        clause.setValue( changeRequest.booleanValue );
                        if ( checkClauseChange( clause, policy.getTarget(), policy.getTargetId() ) ) {
                            policy.getClauses().put( ClauseName.valueOf( changeRequest.clauseName ), clause );
                            this.policies.put( policy.getId(), policy );
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


    public void addPolicy( PolicyChangeRequest changeRequest ) {
        Target target = Target.valueOf( changeRequest.targetName );
        ClauseName clauseName = ClauseName.valueOf( changeRequest.clauseName );
        long targetId = changeRequest.targetId;

        switch ( target ) {
            case POLYPHENY:
                addSpecificPolicy( clauseName, targetId, polyphenyPolicyId, target );
                break;
            case NAMESPACE:
                if ( namespacePolicies.containsKey( targetId ) ) {
                    int policyId = namespacePolicies.get( targetId );
                    addSpecificPolicy( clauseName, targetId, policyId, target );
                }
                break;
            case ENTITY:
                if ( entityPolicies.containsKey( targetId ) ) {
                    int policyId = entityPolicies.get( targetId );
                    addSpecificPolicy( clauseName, targetId, policyId, target );
                }
                break;
            default:
                log.warn( "Not possible to add Policy for target: " + target + ". Not implemented." );
                throw new PolicyRuntimeException( "Not possible to add Policy for target: " + target + ". Not implemented." );
        }
    }


    private void addSpecificPolicy( ClauseName clauseName, long targetId, int policyId, Target target ) {
        Map<ClauseName, Clause> registeredClauses = ClausesRegister.getRegistry();
        if ( this.policies.containsKey( policyId ) ) {
            Policy policy = this.policies.remove( policyId );
            Clause clause = registeredClauses.get( clauseName );
            policy.getClauses().put( clause.getClauseName(), clause );
            this.policies.put( policy.getId(), policy );
        } else {
            Clause clause = registeredClauses.get( clauseName );
            Policy policy = new Policy( target, targetId );
            policy.getClauses().put( clause.getClauseName(), clause );
            this.policies.put( policy.getId(), policy );
        }
    }


    public void deletePolicy( PolicyChangeRequest changeRequest ) {

        Target target = Target.valueOf( changeRequest.targetName );
        ClauseName clauseName = ClauseName.valueOf( changeRequest.clauseName );
        long targetId = changeRequest.targetId;

        switch ( target ) {
            case POLYPHENY:
                deleteSpecificPolicy( polyphenyPolicyId, clauseName );
                break;

            case NAMESPACE:
                if ( namespacePolicies.containsKey( targetId ) ) {
                    int policyId = namespacePolicies.get( targetId );
                    deleteSpecificPolicy( policyId, clauseName );
                }
                break;
            case ENTITY:
                if ( entityPolicies.containsKey( targetId ) ) {
                    int policyId = entityPolicies.get( targetId );
                    deleteSpecificPolicy( policyId, clauseName );
                }
                break;
            default:
                log.warn( "Not possible to delete Policy for target: " + target + ". Not implemented." );
                throw new PolicyRuntimeException( "Not possible to delete Policy for target: " + target + ". Not implemented." );
        }
    }


    private void deleteSpecificPolicy( int policyId, ClauseName clauseName ) {
        if ( this.policies.containsKey( policyId ) ) {
            Policy policy = this.policies.remove( policyId );
            policy.getClauses().remove( clauseName );
            this.policies.put( policy.getId(), policy );
        } else {
            log.warn( "Something went wrong, it should not be possible to delete a policy if this policy was never set." );
            throw new PolicyRuntimeException( "Something went wrong, it should not be possible to delete a policy if this policy was never set." );
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
        List<Integer> storesToCheck = new ArrayList<>();
        List<CatalogPartitionPlacement> partitionPlacements = Catalog.getInstance().getAllPartitionPlacement();
        switch ( clause.getClauseName() ) {
            case FULLY_PERSISTENT:

                AdapterManager.getInstance().getStores().forEach( ( s, dataStore ) -> {
                            if ( !dataStore.isPersistent() ) {
                                storesToCheck.add( dataStore.getAdapterId() );
                            }
                        }
                );

                return checkAllPartitions( target, targetId, storesToCheck, partitionPlacements );

            case PERSISTENT:

                break;
            case ONLY_EMBEDDED:
                AdapterManager.getInstance().getStores().forEach( ( s, dataStore ) -> {
                            if ( dataStore.getDeployMode() != DeployMode.EMBEDDED ) {
                                storesToCheck.add( dataStore.getAdapterId() );
                            }
                        }
                );

                return checkAllPartitions( target, targetId, storesToCheck, partitionPlacements );

            case ONLY_DOCKER:
                AdapterManager.getInstance().getStores().forEach( ( s, dataStore ) -> {
                            if ( dataStore.getDeployMode() != DeployMode.DOCKER ) {
                                storesToCheck.add( dataStore.getAdapterId() );
                            }
                        }
                );

                return checkAllPartitions( target, targetId, storesToCheck, partitionPlacements );

            default:
                throw new PolicyRuntimeException( "Check if it is possible to change clause is not implemented yet." );
        }
        return true;
    }


    private Boolean checkAllPartitions( Target target, Long targetId, List<Integer> storesToCheck, List<CatalogPartitionPlacement> partitionPlacements ) {
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
        return true;
    }


    public <T> List<T> makeDecision( Class<T> clazz, Action action, Long namespaceId, Long entityId ) {
        return makeDecision( clazz, action, namespaceId, entityId, null );
    }


    /**
     * Before polypheny can make a decision it needs to be checked if there is a policy that does not allow the change.
     * For example fully persistent is used, then it is checked that no data is stored on a not persistent store.
     */
    public <T> List<T> makeDecision( Class<T> clazz, Action action, Long namespaceId, Long entityId, T preSelection ) {
        List<Clause> potentiallyInteresting;
        List<DataStore> possibleStores = new ArrayList<>();
        List<ClauseName> clauseNames;

        switch ( action ) {
            case CREATE_TABLE:
                List<Integer> possibleStoreIds = new ArrayList<>();
                // Get all availableStores
                Map<String, DataStore> availableStores = AdapterManager.getInstance().getStores();
                clauseNames = Arrays.asList( ClauseName.FULLY_PERSISTENT, ClauseName.PERSISTENT, ClauseName.ONLY_DOCKER, ClauseName.ONLY_EMBEDDED );

                // Check if there are relevant policies and add it to potentiallyInteresting
                potentiallyInteresting = findPotentiallyInteresting( clauseNames, namespaceId, entityId );

                if ( potentiallyInteresting.isEmpty() ) {
                    for ( DataStore store : availableStores.values() ) {
                        possibleStoreIds.add( store.getAdapterId() );
                    }
                    return (List<T>) possibleStoreIds;
                }

                for ( Clause clause : potentiallyInteresting ) {
                    if ( clause != null ) {
                        if ( clause.getClauseName() == ClauseName.FULLY_PERSISTENT && ((BooleanClause) clause).isValue() ) {
                            for ( DataStore store : availableStores.values() ) {
                                if ( store.isPersistent() ) {
                                    possibleStores.add( store );
                                }
                            }
                        } else if ( clause.getClauseName() == ClauseName.PERSISTENT && ((BooleanClause) clause).isValue() ) {
                            List<DataStore> stores = new ArrayList<>();
                            boolean isPersisten = false;
                            for ( DataStore store : availableStores.values() ) {
                                stores.add( store );
                                if ( store.isPersistent() ) {
                                    isPersisten = true;
                                } else {
                                    possibleStores.remove( store );
                                }
                            }
                            if ( isPersisten ) {
                                possibleStores.addAll( stores );
                            }
                        } else if ( clause.getClauseName() == ClauseName.ONLY_DOCKER && ((BooleanClause) clause).isValue() ) {
                            for ( DataStore store : availableStores.values() ) {
                                if ( store.getDeployMode() == DeployMode.DOCKER ) {
                                    possibleStores.add( store );
                                } else {
                                    possibleStores.remove( store );
                                }
                            }
                        } else if ( clause.getClauseName() == ClauseName.ONLY_EMBEDDED && ((BooleanClause) clause).isValue() ) {
                            for ( DataStore store : availableStores.values() ) {
                                if ( store.getDeployMode() == DeployMode.EMBEDDED ) {
                                    possibleStores.add( store );
                                } else {
                                    possibleStores.remove( store );
                                }
                            }
                        } else {
                            for ( DataStore store : availableStores.values() ) {
                                possibleStoreIds.add( store.getAdapterId() );
                            }
                        }
                    }
                }

                possibleStores.forEach( v -> possibleStoreIds.add( v.getAdapterId() ) );

                return (List<T>) possibleStoreIds;

            case ADD_PLACEMENT:

                clauseNames = Arrays.asList( ClauseName.FULLY_PERSISTENT, ClauseName.PERSISTENT, ClauseName.ONLY_DOCKER, ClauseName.ONLY_EMBEDDED );
                // Check if there are relevant policies and add it to potentiallyInteresting
                potentiallyInteresting = findPotentiallyInteresting( clauseNames, namespaceId, entityId );

                if ( potentiallyInteresting.isEmpty() ) {
                    return Collections.singletonList( preSelection );
                }

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

                clauseNames = Arrays.asList( ClauseName.FULLY_PERSISTENT, ClauseName.PERSISTENT, ClauseName.ONLY_DOCKER, ClauseName.ONLY_EMBEDDED );
                // Check if there are relevant policies and add it to potentiallyInteresting
                potentiallyInteresting = findPotentiallyInteresting( clauseNames, namespaceId, entityId );

                if ( potentiallyInteresting.isEmpty() ) {
                    possibleStores.addAll( (List<DataStore>) preSelection );
                }

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


    private List<Clause> findPotentiallyInteresting( List<ClauseName> clauseNames, Long namespaceId, Long entityId ) {
        List<Clause> interesting = new ArrayList<>();

        for ( ClauseName clauseName : clauseNames ) {
            Clause clausePolypheny = checkClauses( null, clauseName, Target.POLYPHENY );
            if ( clausePolypheny != null ) {
                interesting.add( clausePolypheny );
            }
            Clause clauseNamespace = checkClauses( namespaceId, clauseName, Target.NAMESPACE );
            if ( clauseNamespace != null ) {
                interesting.add( clauseNamespace );
            }
            Clause clauseEntity = checkClauses( entityId, clauseName, Target.ENTITY );
            if ( clauseEntity != null ) {
                interesting.add( clauseEntity );
            }
        }
        return interesting;
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
        return policies.get( policyId ).getClauses().get( clauseName );
    }


    public enum Action {
        CREATE_TABLE, ADD_PARTITIONING, ADD_PLACEMENT

    }

}
