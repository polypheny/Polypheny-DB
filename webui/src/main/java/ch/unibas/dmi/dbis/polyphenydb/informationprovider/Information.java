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

    private String id;
    InformationType type;
    private String informationGroup;


    Information( String id, String group ) {
        this.id = id;
        this.informationGroup = group;
    }


    public String getId() {
        return id;
    }


    public String getGroup() {
        return informationGroup;
    }


    public T ofType( GraphType t ) {
        throwError();
        return (T) this;
    }


    public T setColor( String color ) {
        throwError();
        return (T) this;
    }


    public T setMin( int min ) {
        throwError();
        return (T) this;
    }


    public T setMax( int max ) {
        throwError();
        return (T) this;
    }


    public void updateGraph( GraphData... data ) {
        throwError();
    }


    public void updateHeader( String header ) {
        throwError();
    }


    public void updateHtml( String header ) {
        throwError();
    }


    public void updateLink( String label, String... routerLink ) {
        throwError();
    }


    public void updateProgress( int value ) {
        throwError();
    }


    private void throwError() {
        throw new InformationException( "This method cannot be applied to Information of type " + this.getClass().getSimpleName() );
    }


    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson( this );
    }
}
