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
import java.util.Map.Entry;
import org.polypheny.db.policies.policy.exception.PolicyRuntimeException;
import org.polypheny.db.util.Pair;

public class NumberClause extends Clause {

    private final int value;

    HashMap<Category, Pair<Integer, Integer>> categoryRange;


    public NumberClause( String clauseName, int defaultValue, HashMap<Category, Pair<Integer, Integer>> categoryRange ) {
        super( clauseName );
        this.value = defaultValue;
        this.categoryRange = categoryRange;

    }


    @Override
    public Category getCategory() {

        for ( Entry<Category, Pair<Integer, Integer>> entries : categoryRange.entrySet() ) {
            if ( value >= entries.getValue().left && value <= entries.getValue().right ) {
                return entries.getKey();
            }
        }

        throw new PolicyRuntimeException( "Default Value is not in CategoryRange" );
    }

}
