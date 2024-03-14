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
 * An Information object containing a link to a subpage of the UI.
 */
public class InformationLink extends Information {

    @JsonProperty
    private String label;

    @JsonProperty
    private String[] routerLink;


    /**
     * Constructor
     *
     * @param group      The group this object belongs to
     * @param label      Name of the link
     * @param routerLink Link to a subpage of the UI
     */
    public InformationLink( final InformationGroup group, final String label, final String... routerLink ) {
        this( group.getId(), label, routerLink );
    }


    /**
     * Constructor
     *
     * @param groupId    Id of the group this object belongs to
     * @param label      Name of the link
     * @param routerLink Link to a subpage of the UI
     */
    public InformationLink( final String groupId, final String label, final String... routerLink ) {
        this( UUID.randomUUID().toString(), groupId, label, routerLink );
    }


    /**
     * Constructor
     *
     * @param id         Id of this Information object
     * @param groupId    Id of the group this object belongs to
     * @param label      Name of the link
     * @param routerLink Link to a subpage of the UI
     */
    public InformationLink( final String id, final String groupId, final String label, final String... routerLink ) {
        super( id, groupId );
        this.label = label;
        this.routerLink = routerLink;
    }


    /**
     * Update a InformationLink object
     *
     * @param label      The name of the link when it is displayed
     * @param routerLink Angular route to another subpage of the WebUI
     */
    public void updateLink( final String label, final String... routerLink ) {
        this.label = label;
        this.routerLink = routerLink;
        notifyManager();
    }

}
