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

package org.polypheny.db.policies.policy.policy;

import static org.reflections.Reflections.log;

import java.util.HashMap;
import java.util.List;
import java.util.function.Function;
import lombok.Getter;
import org.polypheny.db.policies.policy.policy.Policies.Target;
import org.polypheny.db.util.Pair;

@Getter
public class NumberClause extends Clause {

    private final int value;

    private final HashMap<ClauseCategory, Pair<Integer, Integer>> categoryRange;


    public NumberClause( ClauseName clauseName, int defaultValue, boolean isDefault, HashMap<ClauseCategory, Pair<Integer, Integer>> categoryRange, ClauseCategory clauseCategory, List<Target> possibleTargets, String description, HashMap<AffectedOperations, Function<List<Object>, List<Object>>> decide, HashMap<Clause, Clause> interfering ) {
        super( clauseName, isDefault, ClauseType.NUMBER, clauseCategory, possibleTargets, description, decide, interfering );
        this.value = defaultValue;
        this.categoryRange = categoryRange;

    }


    @Override
    public <T extends Clause> BooleanClause copy() {
        log.warn("IMPLEMENT copy in Numberclauses");
        return null;
    }


    @Override
    public boolean compareClause( Clause clauseAddition ) {
        log.warn("IMPLEMENT compareClause in Numberclauses");
        return false;
    }

}
