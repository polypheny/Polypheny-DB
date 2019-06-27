/*
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
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.information;


import ch.unibas.dmi.dbis.polyphenydb.information.exception.InformationRuntimeException;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class Information {

    private static final Logger LOG = LoggerFactory.getLogger( InformationManager.class );


    /**
     * The id needs to be unique for every Information object.
     */
    private final String id;

    /**
     * The field type is used by Gson and is needed for the frontend.
     */
    @SuppressWarnings("FieldCanBeLocal")
    final String type;

    /**
     * The field informationGroup consists of the id of the InformationGroup to which it belongs.
     */
    private final String informationGroup;

    /**
     * The information object with lowest uiOrder are rendered first, then those with higher number, then those where uiOrder is null.
     * Field required for GSON.
     */
    @SuppressWarnings("FieldCanBeLocal")
    private int uiOrder;

    /**
     * Sets the information manager instance this information is registered at.
     * Required for notifying the manager about changes.
     */
    private transient InformationManager informationManager;


    /**
     * Constructor
     *
     * @param id Unique id for this Information object
     * @param group The id of the InformationGroup to which this information belongs
     */
    Information( final String id, final String group ) {
        this.id = id;
        this.informationGroup = group;
        this.type = this.getClass().getSimpleName();
    }


    /**
     * Get the id of this Information object.
     *
     * @return id of this Information object
     */
    public String getId() {
        return id;
    }


    /**
     * Get the group id of the group to which this Information object belongs.
     *
     * @return Id of the group this information object belongs to
     */
    public String getGroup() {
        return informationGroup;
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
     * @return The unwraped object
     */
    public <T extends Information> T unwrap( final Class<T> clazz ) {
        if ( clazz.isInstance( this ) ) {
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
        Gson gson = new Gson();
        return gson.toJson( this );
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
    protected void notifyManager() {
        if (informationManager == null) {
            LOG.error( "Requested notification of the information manager but no manager set." );
        } else {
            informationManager.notify( this );
        }
    }

}
