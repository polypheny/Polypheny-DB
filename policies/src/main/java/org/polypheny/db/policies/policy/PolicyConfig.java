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


import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.policies.policy.Clause.Category;
import org.polypheny.db.policies.policy.Clause.PolicyListener;
import org.polypheny.db.policies.policy.Policy.Target;
import org.polypheny.db.policies.policy.PolicyManager.Action;

@Slf4j
public enum PolicyConfig {

    PERSISTENCE(
            Category.PERSISTENT,
            "Policy to ensure only persistence stores are used.",
            false,
            Action.CREATE_TABLE,
            Target.POLYPHENY,
            Collections.singletonList( 1L ) ),

    NON_PERSISTENCE(
            Category.NON_PERSISTENT,
            "Policy to ensure only not persistence stores are used.",
            true,
            Action.CREATE_TABLE,
            Target.POLYPHENY,
            Collections.singletonList( 1L ) );
/*
    PERFORMANCE(
            Category.PERFORMANCE,
            "Policy to ensure good performance.",
            false,
            Action.DEFAULT,
            Target.POLYPHENY,
            Collections.singletonList( 1L ) ),

    TWO_PHASE_COMMIT(
            Category.TWO_PHASE_COMMIT,
            "Policy to ensure two phase commit is used.",
            false,
            Action.DEFAULT,
            Target.POLYPHENY,
            Collections.singletonList( 1L ) );

 */

    private final Category category;
    private final Action action;
    private final String description;
    private final List<Long> targetIds;

    private final PolicyManager policyManager = PolicyManager.getInstance();


    PolicyConfig( final Category category, final String description, final Object defaultValue, final Action action, Target target, List<Long> targetIds ) {
        this.category = category;
        this.description = description;
        this.targetIds = targetIds;
        this.action = action;

        final Clause clause;

        switch ( category ) {
            case PERSISTENT:
                clause = new BooleanClause( action.name(), (Boolean) defaultValue, Category.PERFORMANCE, Category.NON_PERSISTENT );
                break;
            case NON_PERSISTENT:
                clause = new BooleanClause( action.name(), (Boolean) defaultValue, Category.NON_PERSISTENT, Category.PERFORMANCE );
                break;
            default:
                throw new RuntimeException( "Unknown action of policy: " + action.name() );

        }

        // add Clauses to policies, policies are created on start up / creation
        policyManager.registerClause( clause, target, targetIds );
    }


    public void addObserver( final PolicyListener listener ) {
        policyManager.getClause( action.name() ).addObserver( listener );
    }


    public void removeObserver( final PolicyListener listener ) {
        policyManager.getClause( action.name() ).removeObserver( listener );
    }

}
