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


import org.polypheny.db.policies.policy.Clause.Category;
import org.polypheny.db.policies.policy.Policy.Target;
import org.polypheny.db.policies.policy.PolicyManager.Action;

public enum PolicyConfig {

    PERSISTENCE (
            "persistence",
            "Policy to ensure only persistence stores are used.",
            false,
            Action.CREATE_TABLE,
            Target.POLYPHENY),

    NON_PERSISTENCE (
            "nonPersistence",
            "Policy to ensure only not persistence stores are used.",
            false,
            Action.CREATE_TABLE,
            Target.POLYPHENY),

    PERFORMANCE(
            "performance",
            "Policy to ensure good performance.",
            false,
            Action.DEFAULT,
            Target.POLYPHENY),

    TWO_PHASE_COMMIT(
            "twoPhaseCommit",
            "Policy to ensure two phase commit is used.",
            false,
            Action.DEFAULT,
            Target.POLYPHENY);

    private final String key;
    private final String description;

    private final PolicyManager policyManager = PolicyManager.getInstance();

    PolicyConfig(final String key, final String description, final Object defaultValue, final Action action, Target target  ){
        this.key = key;
        this.description = description;

        final Clause clause;

        switch ( key ){
            case "persistence":
                clause = new BooleanClause( action.name(), (Boolean) defaultValue, Category.PERFORMANCE, Category.NON_PERSISTENT );
                break;
            case "nonPersistence":
                clause = new BooleanClause( action.name(), (Boolean) defaultValue, Category.NON_PERSISTENT, Category.PERFORMANCE );
                break;

            default:
                throw new RuntimeException("Unknown action of policy: " + action.name());

        }



        policyManager.registerClause( clause );

    }

}
