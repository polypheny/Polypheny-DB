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


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleOperand;
import java.util.Map;
import java.util.Set;


/**
 * VolcanoPlannerPhaseRuleMappingInitializer describes an inteface for initializing the mapping of {@link VolcanoPlannerPhase}s to sets of rule descriptions.
 *
 * <b>Note:</b> Rule descriptions are obtained via {@link RelOptRule#toString()}. By default they are the class's simple name (e.g. class name sans package),
 * unless the class is an inner class, in which case the default is the inner class's simple name. Some rules explicitly provide alternate descriptions by calling the
 * {@link RelOptRule#RelOptRule(RelOptRuleOperand, String)} constructor.
 */
public interface VolcanoPlannerPhaseRuleMappingInitializer {

    /**
     * Initializes a {@link VolcanoPlannerPhase}-to-rule map. Rules are specified by description (see above). When this method is called, the map will already be pre-initialized with empty sets for each
     * VolcanoPlannerPhase. Implementations must not return having added or removed keys from the map, although it is safe to temporarily add or remove keys.
     *
     * @param phaseRuleMap a {@link VolcanoPlannerPhase}-to-rule map
     */
    void initialize( Map<VolcanoPlannerPhase, Set<String>> phaseRuleMap );
}

