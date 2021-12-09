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

package org.polypheny.db.plan.volcano;


import java.util.Map;
import java.util.Set;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleOperand;


/**
 * VolcanoPlannerPhaseRuleMappingInitializer describes an inteface for initializing the mapping of {@link VolcanoPlannerPhase}s to sets of rule descriptions.
 *
 * <b>Note:</b> Rule descriptions are obtained via {@link AlgOptRule#toString()}. By default they are the class's simple name (e.g. class name sans package),
 * unless the class is an inner class, in which case the default is the inner class's simple name. Some rules explicitly provide alternate descriptions by calling the
 * {@link AlgOptRule#AlgOptRule(AlgOptRuleOperand, String)} constructor.
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

