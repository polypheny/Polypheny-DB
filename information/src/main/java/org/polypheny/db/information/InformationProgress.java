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
 * An Information object that will displayed as a progress bar in the UI
 */
public class InformationProgress extends Information {

    @JsonProperty
    private String label;
    @JsonProperty
    private int value;
    @JsonProperty
    private ProgressColor color = ProgressColor.DYNAMIC;
    @JsonProperty
    private int min = 0;

    @JsonProperty
    private int max = 100;


    /**
     * Constructor
     *
     * @param group The group this information object belongs to
     * @param label Label that will be displayed near the progress bar
     * @param value Value of the progress bar
     */
    public InformationProgress( final InformationGroup group, final String label, final int value ) {
        this( group.getId(), label, value );
    }


    /**
     * Constructor
     *
     * @param groupId The id of the group this information object belongs to
     * @param label   Label that will be displayed near the progress bar
     * @param value   Value of the progress bar
     */
    public InformationProgress( final String groupId, final String label, final int value ) {
        this( UUID.randomUUID().toString(), groupId, label, value );
    }


    /**
     * Constructor
     *
     * @param id      Id of this Information object
     * @param groupId Group to which this information object belongs to
     * @param label   Label that will be displayed near the progress bar
     * @param value   Value of the progress bar
     */
    public InformationProgress( final String id, final String groupId, final String label, final int value ) {
        super( id, groupId );
        this.label = label;
        this.value = value;
    }


    /**
     * Set the color of this progress-bar.
     * The default color is {@link ProgressColor#DYNAMIC}
     *
     * @param color Color of the progress bar
     */
    public InformationProgress setColor( final ProgressColor color ) {
        this.color = color;
        return this;
    }


    /**
     * Set the minimum value of this progress bar.
     *
     * @param min minimum value
     */
    public InformationProgress setMin( final int min ) {
        this.min = min;
        return this;
    }


    /**
     * Set the maximum value of this progress bar.
     *
     * @param max maximum value
     */
    public InformationProgress setMax( final int max ) {
        this.max = max;
        return this;
    }


    /**
     * Update the value of the current state of a progress bar.
     *
     * @param value New value for the progress bar
     */
    public void updateProgress( final int value ) {
        this.value = value;
        notifyManager();
    }


    /**
     * Define the color of a progress bar.
     * DYNAMIC: changes with increasing value, from blue, to green, to yellow, to red.
     */
    public enum ProgressColor {
        DYNAMIC,
        BLUE,
        GREEN,
        YELLOW,
        RED,
        BLACK
    }


}
