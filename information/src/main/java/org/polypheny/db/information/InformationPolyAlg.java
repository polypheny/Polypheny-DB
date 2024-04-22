/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.information;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.UUID;

public class InformationPolyAlg extends Information {

    @JsonProperty
    public String jsonPolyAlg;

    @JsonProperty
    public String planType;


    /**
     * Constructor
     *
     * @param group The InformationGroup to which this information belongs
     */
    public InformationPolyAlg( final InformationGroup group, final String jsonPolyAlg, final PlanType planType ) {
        this( group.getId(), jsonPolyAlg, planType );
        fullWidth( true );
    }


    /**
     * Constructor
     *
     * @param groupId The id of the InformationGroup to which this information belongs
     */
    public InformationPolyAlg( final String groupId, final String jsonPolyAlg, final PlanType planType ) {
        this( UUID.randomUUID().toString(), groupId, jsonPolyAlg, planType );
    }


    /**
     * Constructor
     *
     * @param id Unique id for this information object
     * @param group The id of the InformationGroup to which this information belongs
     */
    public InformationPolyAlg( final String id, final String group, final String jsonPolyAlg, final PlanType planType ) {
        super( id, group );
        this.jsonPolyAlg = jsonPolyAlg;
        this.planType = planType.name();
    }


    public void updatePolyAlg( final String queryPlan ) {
        this.jsonPolyAlg = queryPlan;
        notifyManager();
    }


    public enum PlanType {
        LOGICAL,
        ROUTED,
        PHYSICAL;
    }

}
