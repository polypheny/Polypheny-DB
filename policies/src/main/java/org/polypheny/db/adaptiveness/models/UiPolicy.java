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

package org.polypheny.db.adaptiveness.models;

import org.polypheny.db.adaptiveness.policy.BooleanClause;
import org.polypheny.db.adaptiveness.policy.Clause;
import org.polypheny.db.adaptiveness.policy.Clause.ClauseType;
import org.polypheny.db.adaptiveness.policy.NumberClause;
import org.polypheny.db.adaptiveness.policy.Policies.Target;

public class UiPolicy {

    private final String name;
    private final Target target;
    private final long targetId;
    private final UiClause clause;
    private final ClauseType clauseType;
    private final String description;


    public UiPolicy( String name, Target target, long targetId, Clause clause, ClauseType clauseType, String description ) {
        this.name = name;
        this.target = target;
        this.targetId = targetId;
        this.clause = buildClause( clause, clauseType );
        this.clauseType = clauseType;
        this.description = description;
    }


    private UiClause buildClause( Clause clause, ClauseType clauseType ) {
        switch ( clauseType ) {
            case BOOLEAN:
                return new UiBooleanClause( clause.getClauseName(), clause.getId(), clause.isDefault(), clause.getClauseType(), clause.getClauseCategory(), clause.getDescription(), clause.getPossibleTargets(), ((BooleanClause) clause).isValue() );
            case NUMBER:
                return new UiNumberClause( clause.getClauseName(), clause.getId(), clause.isDefault(), clause.getClauseType(), clause.getClauseCategory(), clause.getDescription(), clause.getPossibleTargets(), ((NumberClause) clause).getValue(), ((NumberClause) clause).getCategoryRange() );
            default:
                return new UiClause( clause.getClauseName(), clause.getId(), clause.isDefault(), clause.getClauseType(), clause.getClauseCategory(), clause.getDescription(), clause.getPossibleTargets() );
        }

    }


}
