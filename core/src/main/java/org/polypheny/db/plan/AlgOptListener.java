/*
 * Copyright 2019-2021 The Polypheny Project
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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.plan;


import java.util.EventListener;
import java.util.EventObject;
import org.polypheny.db.algebra.AlgNode;


/**
 * RelOptListener defines an interface for listening to events which occur during the optimization process.
 */
public interface AlgOptListener extends EventListener {

    /**
     * Notifies this listener that a relational expression has been registered with a particular equivalence class after an equivalence has been either detected or asserted.
     * Equivalence classes may be either logical (all expressions which yield the same result set) or physical (all expressions which yield the same result set with a particular calling convention).
     *
     * @param event details about the event
     */
    void algEquivalenceFound( AlgEquivalenceEvent event );

    /**
     * Notifies this listener that an optimizer rule is being applied to a particular relational expression. This rule is called twice; once before
     * the rule is invoked, and once after. Note that the alg attribute of the event is always the old expression.
     *
     * @param event details about the event
     */
    void ruleAttempted( RuleAttemptedEvent event );

    /**
     * Notifies this listener that an optimizer rule has been successfully applied to a particular relational expression, resulting in a new
     * equivalent expression (relEquivalenceFound will also be called unless the new expression is identical to an existing one). This rule is called
     * twice; once before registration of the new alg, and once after. Note that the alg attribute of the event is always the new expression; to get the
     * old expression, use event.getRuleCall().rels[0].
     *
     * @param event details about the event
     */
    void ruleProductionSucceeded( RuleProductionEvent event );

    /**
     * Notifies this listener that a relational expression is no longer of interest to the planner.
     *
     * @param event details about the event
     */
    void algDiscarded( AlgDiscardedEvent event );

    /**
     * Notifies this listener that a relational expression has been chosen as part of the final implementation of the query plan. After the plan is copmlete, this is called one more time with null for the alg.
     *
     * @param event details about the event
     */
    void algChosen( AlgChosenEvent event );


    /**
     * Event class for abstract event dealing with a relational expression. The source of an event is typically the RelOptPlanner which initiated it.
     */
    abstract class AlgEvent extends EventObject {

        private final AlgNode alg;


        protected AlgEvent( Object eventSource, AlgNode alg ) {
            super( eventSource );
            this.alg = alg;
        }


        public AlgNode getRel() {
            return alg;
        }

    }


    /**
     * Event indicating that a relational expression has been chosen.
     */
    class AlgChosenEvent extends AlgEvent {

        public AlgChosenEvent( Object eventSource, AlgNode alg ) {
            super( eventSource, alg );
        }

    }


    /**
     * Event indicating that a relational expression has been found to be equivalent to an equivalence class.
     */
    class AlgEquivalenceEvent extends AlgEvent {

        private final Object equivalenceClass;
        private final boolean isPhysical;


        public AlgEquivalenceEvent( Object eventSource, AlgNode alg, Object equivalenceClass, boolean isPhysical ) {
            super( eventSource, alg );
            this.equivalenceClass = equivalenceClass;
            this.isPhysical = isPhysical;
        }


        public Object getEquivalenceClass() {
            return equivalenceClass;
        }


        public boolean isPhysical() {
            return isPhysical;
        }

    }


    /**
     * Event indicating that a relational expression has been discarded.
     */
    class AlgDiscardedEvent extends AlgEvent {

        public AlgDiscardedEvent( Object eventSource, AlgNode alg ) {
            super( eventSource, alg );
        }

    }


    /**
     * Event indicating that a planner rule has fired.
     */
    abstract class RuleEvent extends AlgEvent {

        private final AlgOptRuleCall ruleCall;


        protected RuleEvent( Object eventSource, AlgNode alg, AlgOptRuleCall ruleCall ) {
            super( eventSource, alg );
            this.ruleCall = ruleCall;
        }


        public AlgOptRuleCall getRuleCall() {
            return ruleCall;
        }

    }


    /**
     * Event indicating that a planner rule has been attemptedd.
     */
    class RuleAttemptedEvent extends RuleEvent {

        private final boolean before;


        public RuleAttemptedEvent( Object eventSource, AlgNode alg, AlgOptRuleCall ruleCall, boolean before ) {
            super( eventSource, alg, ruleCall );
            this.before = before;
        }


        public boolean isBefore() {
            return before;
        }

    }


    /**
     * Event indicating that a planner rule has produced a result.
     */
    class RuleProductionEvent extends RuleAttemptedEvent {

        public RuleProductionEvent( Object eventSource, AlgNode alg, AlgOptRuleCall ruleCall, boolean before ) {
            super( eventSource, alg, ruleCall, before );
        }

    }

}
