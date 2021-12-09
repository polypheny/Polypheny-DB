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

package org.polypheny.db.tools;


import com.google.common.collect.ImmutableList;
import java.util.Iterator;
import org.polypheny.db.plan.AlgOptRule;


/**
 * Utilities for creating and composing rule sets.
 *
 * @see RuleSet
 */
public class RuleSets {

    private RuleSets() {
    }


    /**
     * Creates a rule set with a given array of rules.
     */
    public static RuleSet ofList( AlgOptRule... rules ) {
        return new ListRuleSet( ImmutableList.copyOf( rules ) );
    }


    /**
     * Creates a rule set with a given collection of rules.
     */
    public static RuleSet ofList( Iterable<? extends AlgOptRule> rules ) {
        return new ListRuleSet( ImmutableList.copyOf( rules ) );
    }


    /**
     * Rule set that consists of a list of rules.
     */
    private static class ListRuleSet implements RuleSet {

        private final ImmutableList<AlgOptRule> rules;


        ListRuleSet( ImmutableList<AlgOptRule> rules ) {
            this.rules = rules;
        }


        @Override
        public int hashCode() {
            return rules.hashCode();
        }


        @Override
        public boolean equals( Object obj ) {
            return obj == this
                    || obj instanceof ListRuleSet
                    && rules.equals( ((ListRuleSet) obj).rules );
        }


        @Override
        public Iterator<AlgOptRule> iterator() {
            return rules.iterator();
        }

    }

}

