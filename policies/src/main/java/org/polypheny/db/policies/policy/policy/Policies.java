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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.policies.policy.policy.Clause.ClauseName;

@Slf4j
public class Policies {

    private final static AtomicInteger atomicId = new AtomicInteger();
    protected static final long TARGET_POLYPHENY = -1;

    @Getter
    private final int id;

    /**
     * For what is this PolicyCategory: entity, namespace or polypheny.
     */
    @Getter
    private final Target target;

    /**
     * Either the table or store id for which the policy is created.
     */
    @Getter
    private final long targetId;


    /**
     * All different clauses
     */
    @Getter
    private final Map<ClauseName, Clause> clauses = new HashMap<>();

    /**
     * All different clauses
     */
    @Getter
    private final Map<ClauseName, Integer> clausesByName = new HashMap<>();


    /**
     * Agreements between the user and polypheny, namespaces or entities.
     *
     * @param target either only for a table, a store or for everything
     * @param targetId id of the selected store, table
     */
    public Policies( Target target, long targetId ) {
        this.id = atomicId.getAndIncrement();
        this.target = target;
        assert targetId >= -1;
        this.targetId = targetId;

        addDefaulClauses();
    }


    public Policies() {
        this( Target.POLYPHENY, TARGET_POLYPHENY );
    }


    /**
     * Add all default policies during creation of a new policy.
     * Only default polypny clauses are added at the moment,
     */
    private void addDefaulClauses() {
        switch ( target ) {
            case POLYPHENY:
                Map<ClauseName, Clause> registeredClauses = ClausesRegister.getBlankRegistry();
                for ( Entry<ClauseName, Clause> clause : registeredClauses.entrySet() ) {
                    if ( clause.getValue().isDefault() ) {
                        clauses.put( clause.getValue().getClauseName(), clause.getValue() );
                        clausesByName.put( clause.getValue().getClauseName(), clause.getValue().getId() );
                    }
                }
                log.warn( "Yeey, default policies for Polypheny are added." );
                break;
            case NAMESPACE:
                log.warn( "No default policies are defined for the: " + target.name() );
                break;
            case ENTITY:
                log.warn( "No default policies are defined for the: " + target.name() );
                break;
            default:
                log.debug( "No default policies are defined for the target: " + target.name() );
        }
    }


    /**
     * Describes for what the policy is used, either only for one table, a store or for everything.
     */
    public enum Target {
        ENTITY, NAMESPACE, POLYPHENY
    }


}

