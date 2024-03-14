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


/**
 * An information object containing a query plan. This class is mainly used for the debugger in the UI.
 */
public class InformationQueryPlan extends Information {

    @JsonProperty
    public String queryPlan;


    /**
     * Constructor
     *
     * @param group The InformationGroup to which this information belongs
     */
    public InformationQueryPlan( final InformationGroup group, final String queryPlan ) {
        this( group.getId(), queryPlan );
        fullWidth( true );
    }


    /**
     * Constructor
     *
     * @param groupId The id of the InformationGroup to which this information belongs
     */
    public InformationQueryPlan( final String groupId, final String queryPlan ) {
        this( UUID.randomUUID().toString(), groupId, queryPlan );
    }


    /**
     * Constructor
     *
     * @param id Unique id for this information object
     * @param group The id of the InformationGroup to which this information belongs
     */
    public InformationQueryPlan( final String id, final String group, final String queryPlan ) {
        super( id, group );
        this.queryPlan = queryPlan;
    }


    public void updateQueryPlan( final String queryPlan ) {
        this.queryPlan = queryPlan;
        notifyManager();
    }

}
