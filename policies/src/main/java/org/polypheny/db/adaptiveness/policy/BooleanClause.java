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

package org.polypheny.db.adaptiveness.policy;

import java.util.HashMap;
import java.util.List;
import java.util.function.Function;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.adaptiveness.policy.Policies.Target;

public class BooleanClause extends Clause {

    @Getter
    @Setter
    private boolean value;


    public BooleanClause(
            ClauseName clauseName,
            boolean defaultValue,
            boolean isDefault,
            ClauseCategory clauseCategory,
            List<Target> possibleTargets,
            String description,
            HashMap<AffectedOperations, Function<List<Object>, List<Object>>> decide,
            HashMap<Clause, Clause> interfering ) {
        super( clauseName, isDefault,  ClauseType.BOOLEAN, clauseCategory, possibleTargets, description, decide, interfering );
        this.value = defaultValue;
    }


    @Override
    public <T extends Clause> BooleanClause copy() {
        return new BooleanClause( getClauseName(), isValue(), isDefault(), getClauseCategory(), getPossibleTargets(), getDescription(), getDecide(), getInterfering() );
    }


    @Override
    public boolean compareClause( Clause clauseAddition ) {
        return getClauseName().equals( clauseAddition.getClauseName() ) &&
                getClauseType().equals( clauseAddition.getClauseType() ) &&
                clauseAddition.isA( ClauseType.BOOLEAN )&&
                isValue() == ((BooleanClause)clauseAddition).isValue();
    }

}
