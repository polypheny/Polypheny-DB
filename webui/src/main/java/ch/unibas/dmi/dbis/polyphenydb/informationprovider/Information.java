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

package ch.unibas.dmi.dbis.polyphenydb.informationprovider;


import ch.unibas.dmi.dbis.polyphenydb.informationprovider.InformationGraph.GraphType;
import com.google.gson.Gson;


public abstract class Information<T extends Information<T>> {

    /**
     * The id needs to be unique for every Information object
     */
    private String id;

    /**
     * The field type is used by Gson and is needed for the Frontend
     */
    InformationType type;

    /**
     * The field informationGroup consists of the id of the InformationGroup to which it belongs.
     */
    private String informationGroup;

    /**
     * uiOrder is used by Gson. Informations with lowest uiOrder are rendered first, then those with higher number, then those where uiOrder is null
     */
    int uiOrder;

    /**
     * Constructor
     *
     * @param id unique id for this Information object
     * @param group id of the InformationGroup to which this information belongs
     */
    Information( String id, String group ) {
        this.id = id;
        this.informationGroup = group;
    }


    /**
     * Get the id of this Information object
     *
     * @return id of this Information object
     */
    public String getId() {
        return id;
    }


    /**
     * Get the group id of the group to which this Information object belongs
     *
     * @return group id of the group to which this Information object belongs
     */
    public String getGroup() {
        return informationGroup;
    }


    /**
     * Define the graph type of a InformationGraph object
     */
    public T ofType( GraphType t ) {
        throwError();
        return (T) this;
    }


    /**
     * Set the color of a progress-bar.
     *
     * @param color possible values:
     * "dynamic" (the color changes with increasing value, from blue, to green, to yellow, to red)
     * "blue" for blue progress bars
     * "green" for green progress bars
     * "yellow" for yellow progress bars
     * "red" for red progress bars
     * "black" for black progress bars
     * default value: "dynamic"
     */
    public T setColor( String color ) {
        throwError();
        return (T) this;
    }



    /**
     * Set the minimum value of a progress bar
     *
     * @param min minimum value
     */
    public T setMin( int min ) {
        throwError();
        return (T) this;
    }


    /**
     * Set the maximum value of a progress bar
     *
     * @param max maximum value
     */
    public T setMax( int max ) {
        throwError();
        return (T) this;
    }


    /**
     * Update the Data of a InformationGraph object
     *
     * @param data new GraphData objects
     */
    public void updateGraph( GraphData... data ) {
        throwError();
    }


    /**
     * Update the text of a InformationHeader
     *
     * @param header new header of a InformationHeader
     */
    public void updateHeader( String header ) {
        throwError();
    }


    /**
     * Update the content of a InformationHtml object
     *
     * @param html new html code to show in the WebUI
     */
    public void updateHtml( String html ) {
        throwError();
    }


    /**
     * Update a InformationLink object
     *
     * @param label the name of the link when it is displayed
     * @param routerLink Angular route to another subpage of the WebUI
     */
    public void updateLink( String label, String... routerLink ) {
        throwError();
    }


    /**
     * Set the order of an Information object
     * Objects with lower numbers are rendered first, the objects with higher numbers, then object where order is not set (0)
     */
    public T setOrder( int order ) {
        this.uiOrder = order;
        return (T) this;
    }


    /**
     * Update the value of the current state of a progress bar
     *
     * @param value new value of a progress bar
     */
    public void updateProgress( int value ) {
        throwError();
    }


    /**
     * Standard error to throw when a method is called that cannot be applied to an Information object of certain type
     */
    private void throwError() {
        throw new InformationException( "This method cannot be applied to Information of type " + this.getClass().getSimpleName() );
    }


    /**
     * Serialize object to JSON string using GSON
     *
     * @return object as JSON string
     */
    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson( this );
    }
}
