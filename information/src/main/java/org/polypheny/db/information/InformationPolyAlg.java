/*
 * Copyright 2019-2025 The Polypheny Project
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
import lombok.Getter;
import lombok.Setter;

public class InformationPolyAlg extends Information {

    @JsonProperty
    public String jsonPolyAlg;

    @JsonProperty
    public String planType;

    @Getter
    @Setter
    private String textualPolyAlg; // this is only not null during testing, where it is desirable to have the human-readable PolyAlgebra


    /**
     * Constructor
     *
     * @param group The InformationGroup to which this information belongs
     */
    public InformationPolyAlg( final InformationGroup group, final String jsonPolyAlg, final PlanType planType ) {
        super( UUID.randomUUID().toString(), group.getId() );
        this.jsonPolyAlg = jsonPolyAlg;
        this.planType = planType.name();
        fullWidth( true );
    }


    public void updatePolyAlg( final String queryPlan ) {
        this.jsonPolyAlg = queryPlan;
        notifyManager();
    }


    public enum PlanType {
        LOGICAL,
        ALLOCATION,
        PHYSICAL
    }

}
