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

package org.polypheny.db.policies.policy.models;

import org.polypheny.db.policies.policy.Clause;
import org.polypheny.db.policies.policy.Clause.ClauseType;
import org.polypheny.db.policies.policy.Policy.Target;

public class DefaultPolicies {

    private final String name;
    private final Target target;
    private final Clause clause;
    private final ClauseType clauseType;

    public DefaultPolicies(String name, Target target, Clause clause, ClauseType clauseType){
        this.name = name;
        this.target = target;
        this.clause = clause;
        this.clauseType = clauseType;
    }


}
