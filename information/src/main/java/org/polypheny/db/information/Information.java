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


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.information.exception.InformationRuntimeException;


@Slf4j
public abstract class Information {

    public static ObjectMapper mapper = new ObjectMapper() {{
        setSerializationInclusion( JsonInclude.Include.NON_NULL );
        configure( DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false );
        configure( SerializationFeature.FAIL_ON_EMPTY_BEANS, false );
        writerWithDefaultPrettyPrinter();
    }};

    /**
     * The id needs to be unique for every Information object.
     */
    @Getter
    @JsonProperty
    private final String id;

    /**
     * The field type is used by Gson and is needed for the frontend.
     */
    @JsonProperty
    String type;

    /**
     * The field informationGroup consists of the id of the InformationGroup to which it belongs.
     */
    @JsonProperty
    private final String groupId;

    /**
     * The information object with lowest uiOrder are rendered first, then those with higher number, then those where uiOrder is null.
     * Field required for GSON.
     */
    @Getter
    @Setter
    @JsonProperty
    private int uiOrder;

    @JsonProperty
    @Accessors(fluent = true)
    @Setter
    @Getter
    public boolean fullWidth;

    /**
     * Sets the information manager instance this information is registered at.
     * Required for notifying the manager about changes.
     */
    private transient InformationManager informationManager;


    /**
     * Constructor
     *
     * @param id Unique id for this Information object
     * @param groupId The id of the InformationGroup to which this information belongs
     */
    Information( final String id, final String groupId ) {
        this.id = id;
        this.groupId = groupId;
        this.type = this.getClass().getSimpleName();
    }


    /**
     * Get the group id of the group to which this Information object belongs.
     *
     * @return Id of the group this information object belongs to
     */
    public String getGroup() {
        return groupId;
    }


    /**
     * Set the order of an Information object.
     * Objects with lower numbers are rendered first, the objects with higher numbers, then objects for which there is no order set (0).
     */
    public Information setOrder( final int order ) {
        this.uiOrder = order;
        return this;
    }


    /**
     * Returns the actual implementation of this information element.
     *
     * @param clazz The
     * @return The unwrapped object
     */
    public <T extends Information> T unwrap( final Class<T> clazz ) {
        if ( clazz.isInstance( this ) ) {
            //noinspection unchecked
            return (T) this;
        } else {
            throw new InformationRuntimeException( "Can not unwrap as " + clazz.getSimpleName() );
        }
    }


    /**
     * Serialize object to JSON string using GSON.
     *
     * @return object as JSON string
     */
    public String asJson() {
        try {
            return mapper.writeValueAsString( this );
        } catch ( JsonProcessingException e ) {
            log.warn( "Error on serializing an Information" );
            return null;
        }
    }


    /**
     * Sets the information manager instance this information is registered at.
     * This is required for notifying the manager about changing information.
     *
     * @return A reference to itself (builder pattern).
     */
    public Information setManager( final InformationManager informationManager ) {
        this.informationManager = informationManager;
        return this;
    }


    /**
     * Notify the information manager about changes of this information object.
     */
    void notifyManager() {
        if ( informationManager != null ) {
            informationManager.notify( this );
        }
    }

}
