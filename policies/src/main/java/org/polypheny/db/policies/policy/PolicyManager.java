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
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.policies.policy.Clause.Category;
import org.polypheny.db.policies.policy.Clause.ClauseName;
import org.polypheny.db.policies.policy.Policy.Target;
import org.polypheny.db.policies.policy.exception.PolicyRuntimeException;
import org.polypheny.db.policies.policy.models.DefaultPolicies;
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
    private final Map<Long, Integer> schemaPolicies = new HashMap<>();

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
            schemaPolicies.put( namespace.id, schemaPolicy.getId() );
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


    public Object getDefaultPolicies() {
        List<DefaultPolicies> defaultPolicies = new ArrayList<>();
        for ( Policy policy : policies.values() ) {
            if ( !policy.getClauses().isEmpty() ) {
                for ( Clause clause : policy.getClauses().values() ) {
                    if ( clause.isDefault() ) {
                        defaultPolicies.add( new DefaultPolicies( clause.getClauseName().name(), policy.getTarget(), clause, clause.getClauseType() ) );
                    }

                }
            }
        }
        return defaultPolicies;
    }


    public void updatePolicies( PolicyChangedRequest changeRequest ) {
        for ( Policy policy : policies.values() ) {
            if ( !policy.getClauses().isEmpty() ) {
                if ( policy.getTarget().name().equals( changeRequest.targetName ) ) {

                    if ( changeRequest.requestType.equals( "BooleanChangeRequest" ) ) {
                        BooleanClause clause = (BooleanClause) policy.getClauses().get( changeRequest.clauseId );
                        clause.setValue( changeRequest.booleanValue );
                        policy.getClauses().put( changeRequest.clauseId, clause );
                        log.warn( "Persistency: " + changeRequest.booleanValue );
                    }
                }
            }
        }


    }


    public <T> List<T> makeDecision( Class<T> clazz, Action action ) {
        return makeDecision( clazz, action, null );
    }


    public <T> List<T> makeDecision( Class<T> clazz, Action action, T preSelection ) {

        switch ( action ) {
            case CREATE_TABLE:
                List<Integer> possibleStores = new ArrayList<>();

                // get all availableStores
                Map<String, DataStore> availableStores = AdapterManager.getInstance().getStores();
                for ( Policy policy : policies.values() ) {
                    if ( !policy.getClauses().isEmpty() ) {
                        for ( Clause clause : policy.getClauses().values() ) {
                            if ( clause.getCategory() == Category.PERSISTENCY && clause.getClauseName() == ClauseName.FULLY_PERSISTENT && ((BooleanClause)clause).isValue()) {
                                for ( DataStore store : availableStores.values() ) {
                                    if ( store.isPersistent() ) {
                                        possibleStores.add( store.getAdapterId() );
                                    }
                                }
                            } else {
                                for ( DataStore store : availableStores.values() ) {
                                    possibleStores.add( store.getAdapterId() );
                                }
                            }
                        }
                    }
                }
                return (List<T>) possibleStores;

            case ADD_PLACEMENT:

                for ( Policy policy : policies.values() ) {
                    if ( !policy.getClauses().isEmpty() ) {
                        for ( Clause clause : policy.getClauses().values() ) {
                            if ( clause.getCategory() == Category.PERSISTENCY && clause.getClauseName() == ClauseName.FULLY_PERSISTENT && ((BooleanClause)clause).isValue()) {
                                DataStore dataStore = AdapterManager.getInstance().getStore( (int) preSelection );
                                if ( dataStore.isPersistent() ) {
                                    return Collections.singletonList( preSelection );
                                }else{
                                    return Collections.emptyList();
                                }
                            }else{
                                return Collections.singletonList( preSelection );
                            }
                        }
                    }
                }

            case ADD_PARTITIONING:
                throw new PolicyRuntimeException( "This is not implemented yet" );

            default:
                throw new PolicyRuntimeException( "Not implemented action was used to make a Decision" );
        }

    }


    public enum Action {
        CREATE_TABLE, ADD_PARTITIONING, ADD_PLACEMENT

    }

}
