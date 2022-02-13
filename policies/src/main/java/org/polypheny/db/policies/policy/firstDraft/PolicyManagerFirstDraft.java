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
/*
package org.polypheny.db.policies.policy.firstDraft;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.policies.policy.Policy;
import org.polypheny.db.policies.policy.PolicyManager.Action;
import org.polypheny.db.policies.policy.firstDraft.PolicyFirstDraft.Target;

public abstract class PolicyManagerFirstDraft {

    public static final int NO_POLYPHENY_POLICY = -1;

    private static PolicyManagerImplFirstDraft INSTANCE = null;


    public static PolicyManagerFirstDraft getInstance() {
        if ( INSTANCE == null ) {
            INSTANCE = new PolicyManagerImplFirstDraft();
        }
        return INSTANCE;
    }


    @Getter
    // id -> Policy
    private Map<Integer, PolicyFirstDraft> policies = new HashMap<>();

    @Getter
    @Setter
    //polyphenyPolicy -> id
    public int polyphenyPolicyId = NO_POLYPHENY_POLICY;

    @Getter
    // map <schemaId -> policyId>
    private final Map<Long, Integer> schemaPolicies = new HashMap<>();

    @Getter
    // map <entityId -> policyId>
    public final Map<Long, Integer> entityPolicies = new HashMap<>();


 */

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
    /*
    public abstract <T> List<T> makeDecision( Class<T> clazz, Action action );

    public abstract <T> List<T> makeDecision( Class<T> clazz, Action action, T preSelection );

   // public abstract <T> List<T> makeDecision( Class<T> clazz, Action action, T preSelection, int returnAmount );

    public abstract void updatePolicies();

   public abstract void registerClause(final ClauseFirstDraft clause, final Target target, final List<Long> targetIds );

    public abstract ClauseFirstDraft getClause( String actionName);

    public abstract void initialize();


}

     */


