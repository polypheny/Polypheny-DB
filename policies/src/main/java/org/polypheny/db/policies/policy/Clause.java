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
import java.util.concurrent.atomic.AtomicInteger;

public abstract class Clause {

    private final AtomicInteger atomicId = new AtomicInteger();
    /**
     * Name of clause.
     */
    private final String clauseName;

    /**
     * Unique id of clause.
     */
    private final int id;


    /**
     * Constructor
     *
     * @param clauseName name for the clause
     */
    protected Clause( String clauseName ) {
        this.id = atomicId.getAndIncrement();
        this.clauseName = clauseName;
        addClauseToPolicy();
    }


    private void addClauseToPolicy() {
        Policy.clauses.put( id, this );
        if(Policy.getClausesByCategories().get( getCategory() ).isEmpty()){
            Policy.getClausesByCategories().put( getCategory(), Collections.singletonList( id ) );
        }else{
            List<Integer> clauses = Policy.getClausesByCategories().remove( getCategory() );
            clauses.add( id );
            Policy.getClausesByCategories().put( getCategory(), clauses );
        }
    }


    public abstract Category getCategory();


    /**
     * different Categories are used to describe the different policies used in Polypheny
     */
    enum Category {
        AVAILABILITY, PERFORMANCE, REDUNDANCY, PERSISTENT, NON_PERSISTENT
    }


}
