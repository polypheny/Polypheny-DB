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
 * An Information object containing html code that will be rendered in the UI.
 */
public class InformationHtml extends Information {

    @JsonProperty
    private String html;


    /**
     * Constructor
     *
     * @param group The group this information element belongs to
     * @param html  The html code
     */
    public InformationHtml( final InformationGroup group, final String html ) {
        this( UUID.randomUUID().toString(), group.getId(), html );
    }


    /**
     * Constructor
     *
     * @param groupId The id of the group this information element belongs to
     * @param html    The html code
     */
    public InformationHtml( final String groupId, final String html ) {
        this( UUID.randomUUID().toString(), groupId, html );
    }


    /**
     * Constructor
     *
     * @param id      The id of this element
     * @param groupId The id of the group this information element belongs to
     * @param html    The html code
     */
    public InformationHtml( final String id, final String groupId, final String html ) {
        super( id, groupId );
        this.html = html;
    }


    /**
     * Update the content of an InformationHtml object.
     *
     * @param html The HTML code to set for this element
     */
    public void updateHtml( final String html ) {
        this.html = html;
        notifyManager();
    }

}
