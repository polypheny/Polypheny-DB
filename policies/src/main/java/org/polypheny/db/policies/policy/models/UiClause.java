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

import java.util.List;
import org.polypheny.db.policies.policy.policy.Clause.ClauseCategory;
import org.polypheny.db.policies.policy.policy.Clause.ClauseName;
import org.polypheny.db.policies.policy.policy.Clause.ClauseType;
import org.polypheny.db.policies.policy.policy.Policies.Target;

public class UiClause {

    private final ClauseName clauseName;
    private final int id;
    private final boolean isDefault;
    private final ClauseType clauseType;
    private final ClauseCategory category;
    private final String description;
    private final List<Target> possibleTargets;


    public UiClause( ClauseName clauseName, int id, boolean isDefault, ClauseType clauseType, ClauseCategory clauseCategory, String description, List<Target> possibleTargets ) {
        this.clauseName = clauseName;
        this.id = id;
        this.isDefault = isDefault;
        this.clauseType = clauseType;
        this.category = clauseCategory;
        this.description = description;
        this.possibleTargets = possibleTargets;
    }

}
