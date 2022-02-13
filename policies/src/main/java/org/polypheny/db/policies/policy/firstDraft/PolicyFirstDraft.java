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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.policies.policy.firstDraft.ClauseFirstDraft;
import org.polypheny.db.policies.policy.firstDraft.ClauseFirstDraft.Category;

@Slf4j
public class PolicyFirstDraft {

    private final AtomicInteger atomicId = new AtomicInteger();
    public static final long TARGET_POLYPHENY = -1;

    @Getter
    private final int id;

    /**
     * for what is this PolicyCategory: table, Store polypheny
     */
    @Getter
    private final Target target;

    /**
     * either the table or store id for which the policy is created
     */
    @Getter
    private final long targetId;


    /**
     * all different clauses
     */
    @Getter
    private final Map<Integer, ClauseFirstDraft> clauses = new HashMap<>();

    @Getter
    private static final Map<Category, List<Integer>> clausesByCategories = new HashMap<>();


    /**
     * agreements between the user and polypheny ar
     *
     * @param target either only for a table, a store or for everything
     * @param targetId id of the selected store, table
     */
    public PolicyFirstDraft( Target target, long targetId ) {
        this.id = atomicId.getAndIncrement();
        this.target = target;
        assert targetId >= -1;
        this.targetId = targetId;

    }


    public PolicyFirstDraft() {
        this( Target.POLYPHENY, TARGET_POLYPHENY );

        //List<Integer> name = PolicyManager.getInstance().makeDecision( int.class, Action.CREATE_TABLE, null );
    }


    /*
    NOT NEEDED, THIS IS DONE IN CREATE DEFAULT POLICY
    private void addPolicy() {
        switch ( target ) {
            case POLYPHENY:
                int polyphenyPolicyId = PolicyManager.getInstance().getPolyphenyPolicyId();
                if ( polyphenyPolicyId == -1 ) {
                    PolicyManager.getInstance().setPolyphenyPolicyId( id );
                } else {
                    throw new PolicyRuntimeException( "There is already a Polypheny Policy with id: " + polyphenyPolicyId + ", it is not possible to create a second one." );
                }
                break;
            case NAMESPACE:
            case ENTITY:
                if ( !(PolicyManager.getInstance().getSchemaPolicies().containsValue( targetId )) ) {
                    PolicyManager.getInstance().getSchemaPolicies().put( targetId, id );
                } else {
                    throw new PolicyRuntimeException( "There is already a Store/Table Policy with id: " + id + ", it is not possible to create a second one." );
                }
                break;
            default:
                throw new PolicyRuntimeException( "Selected Target does not exist within Policy." );

        }

    }


     */

    /**
     * describes for what the policy is used, either only for one table, a store or for everything
     */
    enum Target {
        ENTITY, NAMESPACE, POLYPHENY
    }



}

