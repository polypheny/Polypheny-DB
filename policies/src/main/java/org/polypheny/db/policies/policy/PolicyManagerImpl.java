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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.policies.policy.Clause.Category;
import org.polypheny.db.policies.policy.Policy.Target;
import org.polypheny.db.policies.policy.exception.PolicyRuntimeException;

@Slf4j
public class PolicyManagerImpl extends PolicyManager {

    private final ConcurrentMap<String, Clause> clauses;


    public PolicyManagerImpl() {
        this.clauses = new ConcurrentHashMap<>();
    }


    public void setDefaultPolicies() {
        if ( RuntimeConfig.POLICY.getBoolean() ) {
            createDefaultPolicies();
            addPolicyListener();
        }
    }


    private void createDefaultPolicies() {
        Catalog catalog = Catalog.getInstance();

        // Policy for Polypheny
        getPolicies().put( getPolyphenyPolicyId(), new Policy() );

        // Policy for schemas
        List<CatalogSchema> schemas = catalog.getSchemas( 1, null );
        for ( CatalogSchema schema : schemas ) {
            Policy schemaPolicy = new Policy( Target.STORE, schema.id );
            storePolicies.put( schema.id, schemaPolicy.getId() );
            getPolicies().put( schemaPolicy.getId(), schemaPolicy );
            // Policy for
            List<CatalogTable> childTables = catalog.getTables( schema.id, null );
            for ( CatalogTable childTable : childTables ) {
                Policy tablePolicy = new Policy( Target.STORE, childTable.id );
                storePolicies.put( childTable.id, tablePolicy.getId() );
                getPolicies().put( tablePolicy.getId(), tablePolicy );
            }
        }
    }


    private void addPolicyListener() {
        TrackingListener listener = new TrackingListener();
        PolicyConfig.NON_PERSISTENCE.addObserver( listener );
        PolicyConfig.PERSISTENCE.addObserver( listener );
        //PolicyConfig.TWO_PHASE_COMMIT.addObserver( listener );
    }


    @Override
    public <T> List<T> makeDecision( Class<T> clazz, Action action ) {
        return makeDecision( clazz, action, null );
    }


    @Override
    public <T> List<T> makeDecision( Class<T> clazz, Action action, T preSelection ) {
        return makeDecision( clazz, action, preSelection, 1 );
    }


    @Override
    public <T> List<T> makeDecision( Class<T> clazz, Action action, T preSelection, int returnAmount ) {

        switch ( action ) {
            case CREATE_TABLE:
                List<Integer> possibleStores = new ArrayList<>();

                //4. what are my possibilities?
                Map<String, DataStore> availableStores = AdapterManager.getInstance().getStores();

                // 1. get PolphenyPolyicyId -> only the polypheny policy is interesting
                if ( polyphenyPolicyId != NO_POLYPHENY_POLICY ) {
                    //2. get all clauses of interest
                    List<Integer> interestingPersistentClauses = policies.get( polyphenyPolicyId ).getClausesByCategories().get( Category.PERSISTENT );
                    List<Integer> interestingNonPersistentClauses = policies.get( polyphenyPolicyId ).getClausesByCategories().get( Category.NON_PERSISTENT );

                    //3. fixed persistent or fixed not?

                    // 5. which matches selection best?
                    if ( !interestingPersistentClauses.isEmpty() ) {
                        for ( DataStore store : availableStores.values() ) {
                            if ( store.isPersistent() ) {
                                possibleStores.add( store.getAdapterId() );
                            }
                        }
                    } else if ( !interestingNonPersistentClauses.isEmpty() ) {
                        for ( DataStore store : availableStores.values() ) {
                            if ( !store.isPersistent() ) {
                                possibleStores.add( store.getAdapterId() );
                            }
                        }
                    }
                }
                return (List<T>) possibleStores;
            default:
                throw new PolicyRuntimeException( "Not implemented action was used to make a Decision" );
        }

    }


    @Override
    public void updatePolicies() {

    }


    @Override
    public void registerClause( Clause clause, Target target, List<Long> targetIds ) {
        Policy policy;

        if ( target == Target.POLYPHENY ) {
            policy = getPolicies().remove( getPolyphenyPolicyId() );
            policy.getClauses().put( clause.getId(), clause );
            getPolicies().put( getPolyphenyPolicyId(), policy );
            clauses.put( clause.getClauseName(), clause );

        } else if ( target == Target.STORE || target == Target.TABLE ) {
            for ( Long targetId : targetIds ) {
                policy = getPolicies().remove( clause.getId() );
                policy.getClauses().put( clause.getId(), clause );
                getPolicies().put( clause.getId(), policy );
                storePolicies.put( targetId, clause.getId() );
                clauses.put( clause.getClauseName(), clause );
            }
        }
    }


    @Override
    public Clause getClause( String actionName ) {
        return clauses.get( actionName );
    }


    class TrackingListener implements Clause.PolicyListener {

        @Override
        public void onConfigChange( Clause c ) {
            registerTrackingToggle();
        }


        private void registerTrackingToggle() {
            log.warn( "Tracking Listener for Policy" );
        }


    }

}
