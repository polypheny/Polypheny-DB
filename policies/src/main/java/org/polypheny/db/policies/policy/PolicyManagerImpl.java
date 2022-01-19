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
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.policies.policy.Clause.Category;
import org.polypheny.db.policies.policy.exception.PolicyRuntimeException;

public class PolicyManagerImpl extends PolicyManager {

    public PolicyManagerImpl() {

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
                if ( polyphenyPolicy != NO_POLYPHENY_POLICY ) {
                    //2. get all clauses of interest
                    List<Integer> interestingPersistentClauses = policies.get( polyphenyPolicy ).getClausesByCategories().get( Category.PERSISTENT );
                    List<Integer> interestingNonPersistentClauses = policies.get( polyphenyPolicy ).getClausesByCategories().get( Category.NON_PERSISTENT );

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

/*
    public ImmutableList<DataStore> getPolicyInformation( Map<String, DataStore> stores, boolean isSinglePlacement) {
        ArrayList<DataStore> persistentStores = new ArrayList<>();

        for( Policy policy : policies.values()){
            if( policy.getSelectedFor() == Target.POLYPHENY && policy.getCategory() == Category.AVAILABILITY){
                if(((BooleanClause) policy).isPersistence()){

                    for ( DataStore store : stores.values() ) {
                        if ( store.isPersistent() ) {
                            System.out.println("add to persistent store");
                            persistentStores.add( store );
                            if(isSinglePlacement){
                                return ImmutableList.copyOf(persistentStores);
                            }
                        }
                    }

                    if(persistentStores.isEmpty()){
                        System.out.println("no persistent store");
                        if(isSinglePlacement){
                            for ( DataStore store : stores.values() ) {
                                return ImmutableList.of(store);
                            }
                        }
                        persistentStores.addAll( stores.values() );
                    }

                    return ImmutableList.copyOf(persistentStores);


                }else if(!((BooleanClause) policy).isPersistence()){
                    for ( DataStore store : stores.values() ) {
                        if ( !store.isPersistent() ) {
                            persistentStores.add( store );
                            if(isSinglePlacement){
                                return ImmutableList.copyOf(persistentStores);
                            }
                        }
                    }

                    if(persistentStores.isEmpty()){
                        System.out.println("no non persistent store");
                        if(isSinglePlacement){
                            for ( DataStore store : stores.values() ) {
                                return ImmutableList.of(store);
                            }
                        }
                        persistentStores.addAll( stores.values() );
                    }

                    return ImmutableList.copyOf(persistentStores);
                }
            }
        }

        persistentStores.addAll( stores.values() );
        return ImmutableList.copyOf(persistentStores);
    }


 */


}
