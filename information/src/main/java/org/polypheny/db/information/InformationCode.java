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
import lombok.EqualsAndHashCode;
import lombok.Value;


/**
 * An Information object containing code that will be rendered in an ace editor in the UI
 */
@EqualsAndHashCode(callSuper = true)
@Value
public class InformationCode extends Information {

    @JsonProperty
    public String code;

    @JsonProperty
    public String language;


    /**
     * Constructor
     *
     * @param group The group this information element belongs to
     * @param code Code that should be rendered in an ace editor in the UI
     */
    public InformationCode( final InformationGroup group, final String code ) {
        this( UUID.randomUUID().toString(), group.getId(), code, "java" );
        fullWidth( true );
    }


    /**
     * Constructor
     *
     * @param group The group this information element belongs to
     * @param code Code that should be rendered in an ace editor in the UI
     * @param language The language for the ace syntax highlighting
     */
    public InformationCode( final InformationGroup group, final String code, final String language ) {
        this( UUID.randomUUID().toString(), group.getId(), code, language );
    }


    /**
     * Constructor
     *
     * @param id The id of this information element
     * @param groupId The group this information element belongs to
     * @param code Code that should be rendered in an ace editor in the UI
     * @param language The language for the ace syntax highlighting
     */
    public InformationCode( final String id, final String groupId, final String code, final String language ) {
        super( id, groupId );
        this.code = code;
        this.language = language;
    }


}
