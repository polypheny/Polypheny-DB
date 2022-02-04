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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

public abstract class PolicyManager {

    public static final int NO_POLYPHENY_POLICY = -1;

    private static PolicyManagerImpl INSTANCE = null;


    public static PolicyManager getInstance() {
        if ( INSTANCE == null ) {
            INSTANCE = new PolicyManagerImpl();
        }
        return INSTANCE;
    }


    @Getter
    // id -> Policy
    public Map<Integer, Policy> policies = new HashMap<>();

    @Getter
    @Setter
    //polyphenyPolicy -> id
    public int polyphenyPolicy = NO_POLYPHENY_POLICY;

    @Getter
    // map <storeId -> policyId>
    public final Map<Long, Integer> storePolicies = new HashMap<>();

    @Getter
    // map <entityId -> policyId>
    public final Map<Long, Integer> entityPolicies = new HashMap<>();


    /**
     * if a decision within polypheny needs to be made this method is called
     * depending on the Action makeDecision returns a list of options in consideration of the policies previously
     * defined, if there are no policies all possibilities are returned
     *
     * @param clazz needed return type
     * @param action what decision is made
     * @param <T> information if the use has something preselected
     * @return
     */
    public abstract <T> List<T> makeDecision( Class<T> clazz, Action action );

    public abstract <T> List<T> makeDecision( Class<T> clazz, Action action, T preSelection );

    public abstract <T> List<T> makeDecision( Class<T> clazz, Action action, T preSelection, int returnAmount );

    public abstract void updatePolicies();


    enum Action {
        CREATE_TABLE;

    }

}
