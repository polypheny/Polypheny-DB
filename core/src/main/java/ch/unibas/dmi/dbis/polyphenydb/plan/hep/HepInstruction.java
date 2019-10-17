/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.plan.hep;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;


/**
 * HepInstruction represents one instruction in a HepProgram. The actual instruction set is defined here via inner classes; if these grow too big, they should be moved out to top-level classes.
 */
abstract class HepInstruction {

    void initialize( boolean clearCache ) {
    }


    // typesafe dispatch via the visitor pattern
    abstract void execute( HepPlanner planner );


    /**
     * Instruction that executes all rules of a given class.
     *
     * @param <R> rule type
     */
    static class RuleClass<R extends RelOptRule> extends HepInstruction {

        Class<R> ruleClass;

        /**
         * Actual rule set instantiated during planning by filtering all of the planner's rules through ruleClass.
         */
        Set<RelOptRule> ruleSet;


        @Override
        void initialize( boolean clearCache ) {
            if ( !clearCache ) {
                return;
            }
            ruleSet = null;
        }


        @Override
        void execute( HepPlanner planner ) {
            planner.executeInstruction( this );
        }
    }


    /**
     * Instruction that executes all rules in a given collection.
     */
    static class RuleCollection extends HepInstruction {

        /**
         * Collection of rules to apply.
         */
        Collection<RelOptRule> rules;


        @Override
        void execute( HepPlanner planner ) {
            planner.executeInstruction( this );
        }
    }


    /**
     * Instruction that executes converter rules.
     */
    static class ConverterRules extends HepInstruction {

        boolean guaranteed;

        /**
         * Actual rule set instantiated during planning by filtering all of the planner's rules, looking for the desired converters.
         */
        Set<RelOptRule> ruleSet;


        @Override
        void execute( HepPlanner planner ) {
            planner.executeInstruction( this );
        }
    }


    /**
     * Instruction that finds common relational sub-expressions.
     */
    static class CommonRelSubExprRules extends HepInstruction {

        Set<RelOptRule> ruleSet;


        @Override
        void execute( HepPlanner planner ) {
            planner.executeInstruction( this );
        }
    }


    /**
     * Instruction that executes a given rule.
     */
    static class RuleInstance extends HepInstruction {

        /**
         * Description to look for, or null if rule specified explicitly.
         */
        String ruleDescription;

        /**
         * Explicitly specified rule, or rule looked up by planner from description.
         */
        RelOptRule rule;


        @Override
        void initialize( boolean clearCache ) {
            if ( !clearCache ) {
                return;
            }

            if ( ruleDescription != null ) {
                // Look up anew each run.
                rule = null;
            }
        }


        @Override
        void execute( HepPlanner planner ) {
            planner.executeInstruction( this );
        }
    }


    /**
     * Instruction that sets match order.
     */
    static class MatchOrder extends HepInstruction {

        HepMatchOrder order;


        @Override
        void execute( HepPlanner planner ) {
            planner.executeInstruction( this );
        }
    }


    /**
     * Instruction that sets match limit.
     */
    static class MatchLimit extends HepInstruction {

        int limit;


        @Override
        void execute( HepPlanner planner ) {
            planner.executeInstruction( this );
        }
    }


    /**
     * Instruction that executes a sub-program.
     */
    static class Subprogram extends HepInstruction {

        HepProgram subprogram;


        @Override
        void initialize( boolean clearCache ) {
            subprogram.initialize( clearCache );
        }


        @Override
        void execute( HepPlanner planner ) {
            planner.executeInstruction( this );
        }
    }


    /**
     * Instruction that begins a group.
     */
    static class BeginGroup extends HepInstruction {

        EndGroup endGroup;


        @Override
        void initialize( boolean clearCache ) {
        }


        @Override
        void execute( HepPlanner planner ) {
            planner.executeInstruction( this );
        }
    }


    /**
     * Instruction that ends a group.
     */
    static class EndGroup extends HepInstruction {

        /**
         * Actual rule set instantiated during planning by collecting grouped rules.
         */
        Set<RelOptRule> ruleSet;

        boolean collecting;


        @Override
        void initialize( boolean clearCache ) {
            if ( !clearCache ) {
                return;
            }

            ruleSet = new HashSet<>();
            collecting = true;
        }


        @Override
        void execute( HepPlanner planner ) {
            planner.executeInstruction( this );
        }
    }
}

