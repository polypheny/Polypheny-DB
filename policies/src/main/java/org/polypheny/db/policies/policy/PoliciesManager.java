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

import static org.polypheny.db.policies.policy.Policies.TARGET_POLYPHENY;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.policies.policy.Clause.AffectedOperations;
import org.polypheny.db.policies.policy.Clause.ClauseCategory;
import org.polypheny.db.policies.policy.Clause.ClauseName;
import org.polypheny.db.policies.policy.Clause.ClauseType;
import org.polypheny.db.policies.policy.Policies.Target;
import org.polypheny.db.policies.policy.exception.PolicyRuntimeException;
import org.polypheny.db.policies.policy.models.PolicyChangeRequest;
import org.polypheny.db.policies.policy.models.UiPolicy;
import org.polypheny.db.util.Pair;

@Slf4j
public class PoliciesManager {

    private static PoliciesManager INSTANCE = null;


    public static PoliciesManager getInstance() {
        if ( INSTANCE == null ) {
            INSTANCE = new PoliciesManager();
        }
        return INSTANCE;
    }


    // id -> Policy
    private final Map<Integer, Policies> policies = new HashMap<>();

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
        Policies polyphenyPolicies = new Policies();
        polyphenyPolicyId = polyphenyPolicies.getId();
        policies.put( polyphenyPolicies.getId(), polyphenyPolicies );

        // Policy for each namespace
        List<CatalogSchema> namespaces = catalog.getSchemas( 1, null );
        for ( CatalogSchema namespace : namespaces ) {
            Policies schemaPolicies = new Policies( Target.NAMESPACE, namespace.id );
            namespacePolicies.put( namespace.id, schemaPolicies.getId() );
            policies.put( schemaPolicies.getId(), schemaPolicies );
            // Policy for each entity
            List<CatalogTable> entities = catalog.getTables( namespace.id, null );
            for ( CatalogTable entity : entities ) {
                Policies tablePolicies = new Policies( Target.ENTITY, entity.id );
                entityPolicies.put( entity.id, tablePolicies.getId() );
                policies.put( tablePolicies.getId(), tablePolicies );
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
    public List<UiPolicy> getClause( Pair<Target, Long> target ) {
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
        Policies policy = this.policies.get( policyId );
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
    public List<UiPolicy> getPossibleClause( Pair<Target, Long> target ) {
        List<UiPolicy> targetPolicies = new ArrayList<>();
        long targetId = target.right;
        switch ( target.left ) {
            case POLYPHENY:
                targetPolicies.addAll( getRelevantPolicies( polyphenyPolicyId, targetId, target.left ) );
                break;
            case NAMESPACE:
                if ( !namespacePolicies.containsKey( targetId ) ) {
                    Policies schemaPolicies = new Policies( Target.NAMESPACE, targetId );
                    namespacePolicies.put( targetId, schemaPolicies.getId() );
                    policies.put( schemaPolicies.getId(), schemaPolicies );
                }
                targetPolicies.addAll( getRelevantPolicies( namespacePolicies.get( targetId ), targetId, target.left ) );
                break;
            case ENTITY:
                if ( !entityPolicies.containsKey( targetId ) ) {
                    Policies tablePolicies = new Policies( Target.ENTITY, targetId );
                    entityPolicies.put( targetId, tablePolicies.getId() );
                    policies.put( tablePolicies.getId(), tablePolicies );
                }
                targetPolicies.addAll( getRelevantPolicies( entityPolicies.get( targetId ), targetId, target.left ) );
                break;
            default:
                log.warn( "This Target is not implemented yet." );
                throw new PolicyRuntimeException( "This Target is not implemented yet." );
        }
        return targetPolicies;
    }


    private List<UiPolicy> getRelevantPolicies( int policyId, long targetId, Target target ) {
        Map<ClauseName, Clause> registeredClauses = ClausesRegister.getBlankRegistry();
        List<UiPolicy> relevantPolicies = new ArrayList<>();
        for ( Clause clause : registeredClauses.values() ) {
            if ( clause.getPossibleTargets().contains( target ) && !this.policies.get( policyId ).getClauses().containsKey( clause.getClauseName() ) ) {
                relevantPolicies.add( new UiPolicy( clause.getClauseName().name(), target, targetId, clause, clause.getClauseType(), clause.getDescription() ) );
            }
        }
        return relevantPolicies;
    }


    public void updateClauses( PolicyChangeRequest changeRequest ){

        Target target = Target.valueOf( changeRequest.targetName );
        Clause clause = null;

        for ( Policies policies : this.policies.values() ) {
            if ( !policies.getClauses().isEmpty() ) {
                if ( policies.getTarget() == target ) {
                    if ( changeRequest.requestType.equals( "BooleanChangeRequest" ) ) {
                        clause = policies.getClauses().get( ClauseName.valueOf( changeRequest.clauseName ) );
                        ((BooleanClause) clause).setValue( changeRequest.booleanValue );
                    }

                    assert clause != null;
                    if ( checkClauseChange( clause, policies.getTarget(), policies.getTargetId() ) ) {
                        policies.getClauses().put( ClauseName.valueOf( changeRequest.clauseName ), clause );
                        this.policies.put( policies.getId(), policies );
                    } else {
                        log.warn( "Persistency not possible to change." );
                        throw new PolicyRuntimeException( "Not possible to change this clause because the policies can not be guaranteed anymore." );
                    }
                }
            }
        }
    }


    public void addClause( PolicyChangeRequest changeRequest ) {
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
        Map<ClauseName, Clause> registeredClauses = ClausesRegister.getBlankRegistry();
        if ( this.policies.containsKey( policyId ) ) {
            Policies policies = this.policies.remove( policyId );
            Clause clause = registeredClauses.get( clauseName );
            policies.getClauses().put( clause.getClauseName(), clause );
            this.policies.put( policies.getId(), policies );
        } else {
            Clause clause = registeredClauses.get( clauseName );
            Policies policies = new Policies( target, targetId );
            policies.getClauses().put( clause.getClauseName(), clause );
            this.policies.put( policies.getId(), policies );
        }
    }


    public void deleteClause( PolicyChangeRequest changeRequest ) {

        Target target = Target.valueOf( changeRequest.targetName );
        ClauseName clauseName = ClauseName.valueOf( changeRequest.clauseName );
        long targetId = changeRequest.targetId;

        switch ( target ) {
            case POLYPHENY:
                deleteSpecificClause( polyphenyPolicyId, clauseName );
                break;

            case NAMESPACE:
                if ( namespacePolicies.containsKey( targetId ) ) {
                    int policyId = namespacePolicies.get( targetId );
                    deleteSpecificClause( policyId, clauseName );
                }
                break;
            case ENTITY:
                if ( entityPolicies.containsKey( targetId ) ) {
                    int policyId = entityPolicies.get( targetId );
                    deleteSpecificClause( policyId, clauseName );
                }
                break;
            default:
                log.warn( "Not possible to delete Policy for target: " + target + ". Not implemented." );
                throw new PolicyRuntimeException( "Not possible to delete Policy for target: " + target + ". Not implemented." );
        }
    }


    private void deleteSpecificClause( int policyId, ClauseName clauseName ) {
        if ( this.policies.containsKey( policyId ) ) {
            Policies policies = this.policies.remove( policyId );
            policies.getClauses().remove( clauseName );
            this.policies.put( policies.getId(), policies );
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
    private boolean checkClauseChange( Clause clause, Target target, Long targetId ){
        switch ( clause.getClauseCategory() ) {
            case STORE:
                return checkStoreClauses( clause, target, targetId ) && checkActiveClauses( clause, target, targetId );
            default:
                throw new PolicyRuntimeException( "Category is not yet implemented: " + clause.getClauseCategory() );
        }

    }


    private boolean checkActiveClauses( Clause clause, Target target, Long targetId ) {
        switch ( target ) {
            case POLYPHENY:
                return checkClauses( clause, polyphenyPolicyId );
            case NAMESPACE:
                return checkClauses( clause, polyphenyPolicyId ) && checkClauses( clause, namespacePolicies.get( targetId ) );
            case ENTITY:
                return checkClauses( clause, polyphenyPolicyId ) && checkClauses( clause, namespacePolicies.get( targetId ) ) && checkClauses( clause, entityPolicies.get( targetId ) );
        }
        return false;
    }


    private boolean checkClauses( Clause clause, int policyId ) {
        List<Clause> clauses = new ArrayList<>( policies.get( policyId ).getClauses().values() );
        for ( Clause clauseToCheck : clauses ) {
            for ( Entry<Clause, Clause> interfering : clauseToCheck.getInterfering().entrySet() ) {
                if ( clauseToCheck.compareClause( interfering.getKey() ) && interfering.getValue().compareClause( clause ) ) {
                    return false;
                }
            }
        }
        return true;
    }


    private boolean checkStoreClauses( Clause clause, Target target, Long targetId ) {

        List<CatalogPartitionPlacement> partitionPlacements = Catalog.getInstance().getAllPartitionPlacement();
        Map<String, DataStore> allStores = AdapterManager.getInstance().getStores();
        List<Object> possibleStores = new ArrayList<>( allStores.values() );

        if ( clause.getClauseCategory() == ClauseCategory.STORE && clause.isA( ClauseType.BOOLEAN ) && ((BooleanClause) clause).isValue() ) {
            for ( Entry<AffectedOperations, Function<List<Object>, List<Object>>> findStores : clause.getDecide().entrySet() ) {
                possibleStores = findStores.getValue().apply( possibleStores );
            }
        }
        if(allStores.size() == possibleStores.size()){
            return true;
        }

        if(possibleStores.isEmpty()){
            throw new PolicyRuntimeException( "It is not possible to use this policy because there is no store that holds the criteria: " + clause.getClauseName() );
        }

        List<Integer> adapterIds = new ArrayList<>();
        possibleStores.forEach( ( s ) -> adapterIds.add( ((DataStore) s).getAdapterId() ) );
        for ( CatalogPartitionPlacement partitionPlacement : partitionPlacements ) {
            switch ( target ) {
                case POLYPHENY:
                    if ( !adapterIds.contains( partitionPlacement.adapterId ) ) {
                        return false;
                    }
                    break;
                case NAMESPACE:
                    if ( !adapterIds.contains( partitionPlacement.adapterId ) && partitionPlacement.schemaId == targetId ) {
                        return false;
                    }
                    break;
                case ENTITY:
                    if ( !adapterIds.contains( partitionPlacement.adapterId ) && partitionPlacement.tableId == targetId ) {
                        return false;
                    }
                    break;
                default:
                    log.warn( "target is not specified in checkStoreclause" );
            }
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

        List<ClauseName> clauseNames;

        switch ( action ) {
            case CHECK_STORES:
                List<Object> possibleStores;
                clauseNames = Arrays.asList( ClauseName.FULLY_PERSISTENT, ClauseName.PERSISTENT, ClauseName.ONLY_DOCKER, ClauseName.ONLY_EMBEDDED );
                // Check if there are relevant policies and add it to potentiallyInteresting
                potentiallyInteresting = findPotentiallyInteresting( clauseNames, namespaceId, entityId );

                if ( preSelection != null ) {
                    possibleStores = Collections.singletonList( preSelection );
                } else {
                    // Get all availableStores
                    Map<String, DataStore> availableStores = AdapterManager.getInstance().getStores();
                    possibleStores = new ArrayList<>( availableStores.values() );
                }

                if ( potentiallyInteresting.isEmpty() ) {
                    return (List<T>) possibleStores;
                }

                for ( Clause clause : potentiallyInteresting ) {
                    if ( clause.getClauseCategory() == ClauseCategory.STORE && clause.isA( ClauseType.BOOLEAN ) && ((BooleanClause) clause).isValue() ) {
                        for ( Entry<AffectedOperations, Function<List<Object>, List<Object>>> findStores : clause.getDecide().entrySet() ) {
                            possibleStores = findStores.getValue().apply( possibleStores );
                        }
                    }
                }
                return (List<T>) possibleStores;

            case ADD_PARTITIONING:
/*
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
                return (List<T>) possibleStores;*/
                return null;
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
        CHECK_STORES, ADD_PARTITIONING,

    }

}
