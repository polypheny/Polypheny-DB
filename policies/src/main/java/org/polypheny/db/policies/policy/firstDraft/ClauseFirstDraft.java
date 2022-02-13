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

package org.polypheny.db.policies.policy.firstDraft;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;

public abstract class ClauseFirstDraft {

    private final AtomicInteger atomicId = new AtomicInteger();
    /**
     * Name of clause.
     */
    @Getter
    private final String clauseName;

    /**
     * Unique id of clause.
     */
    @Getter
    private final int id;

    /**
     * List of observers.
     */
    private final Map<Integer, PolicyListener> listeners = new HashMap<>();


    /**
     * Constructor
     *
     * @param clauseName name for the clause
     */
    protected ClauseFirstDraft( String clauseName ) {
        this.id = atomicId.getAndIncrement();
        this.clauseName = clauseName;

    }

/*
    private void addClauseToPolicy() {
        Policy.getClauses().put( id, this );
        if(Policy.getClausesByCategories().get( getCategory() ).isEmpty()){
            Policy.getClausesByCategories().put( getCategory(), Collections.singletonList( id ) );
        }else{
            List<Integer> clauses = Policy.getClausesByCategories().remove( getCategory() );
            clauses.add( id );
            Policy.getClausesByCategories().put( getCategory(), clauses );
        }
    }

 */


    public abstract Category getCategory();


    /**
     * Add an observer for this config element.
     *
     * @param listener Observer to add
     * @return Clause
     */
    public ClauseFirstDraft addObserver( final PolicyListener listener ) {

        this.listeners.put( listener.hashCode(), listener );
        return this;
    }

    public ClauseFirstDraft removeObserver( final PolicyListener listener ) {
        this.listeners.remove( listener.hashCode() );
        return this;
    }


    /**
     * Notify observers
     */
    protected void notifyConfigListeners() {
        for ( PolicyListener listener : listeners.values() ) {
            listener.onConfigChange( this );
        }
    }


    /**
     * different Categories are used to describe the different policies used in Polypheny
     */
    public enum Category {
        AVAILABILITY, PERFORMANCE, REDUNDANCY, PERSISTENT, NON_PERSISTENT, TWO_PHASE_COMMIT
    }

    public interface PolicyListener {

        void onConfigChange( ClauseFirstDraft c );

    }


}
