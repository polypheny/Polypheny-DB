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

package ch.unibas.dmi.dbis.polyphenydb.plan.volcano;


import java.util.Map;
import java.util.Set;


/**
 * ChainedPhaseRuleMappingInitializer is an abstract implementation of {@link VolcanoPlannerPhaseRuleMappingInitializer} that allows additional rules to be layered on top of
 * those configured by a subordinate {@link VolcanoPlannerPhaseRuleMappingInitializer}.
 *
 * @see VolcanoPlannerPhaseRuleMappingInitializer
 */
public abstract class ChainedPhaseRuleMappingInitializer implements VolcanoPlannerPhaseRuleMappingInitializer {

    private final VolcanoPlannerPhaseRuleMappingInitializer subordinate;


    public ChainedPhaseRuleMappingInitializer( VolcanoPlannerPhaseRuleMappingInitializer subordinate ) {
        this.subordinate = subordinate;
    }


    public final void initialize( Map<VolcanoPlannerPhase, Set<String>> phaseRuleMap ) {
        // Initialize subordinate's mappings.
        subordinate.initialize( phaseRuleMap );

        // Initialize our mappings.
        chainedInitialize( phaseRuleMap );
    }


    /**
     * Extend this method to provide phase-to-rule mappings beyond what is provided by this initializer's subordinate.
     *
     * When this method is called, the map will already be pre-initialized with empty sets for each VolcanoPlannerPhase. Implementations must not return having added or removed keys from the map,
     * although it is safe to temporarily add or remove keys.
     *
     * @param phaseRuleMap the {@link VolcanoPlannerPhase}-rule description map
     * @see VolcanoPlannerPhaseRuleMappingInitializer
     */
    public abstract void chainedInitialize( Map<VolcanoPlannerPhase, Set<String>> phaseRuleMap );
}

