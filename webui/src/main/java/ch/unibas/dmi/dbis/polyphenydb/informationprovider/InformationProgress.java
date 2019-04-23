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


public class InformationProgress extends Information {

    private String label;
    private int value;
    private String color = "dynamic";
    private int min = 0;
    private int max = 100;


    public InformationProgress( String id, String group, String label, int value ) {
        super( id, group );
        this.type = InformationType.PROGRESS;
        this.label = label;
        this.value = value;
    }


    /**
     * @param color default: dynamic
     * info/blue ("info" or "blue" will give you a blue progress bar)
     * success/green
     * warning/yellow
     * danger/red
     * dark/black
     * (see render-item.component.ts -> setProgressColor() in Webui)
     */
    public InformationProgress setColor( String color ) {
        this.color = color;
        return this;
    }


    public InformationProgress setMin( int min ) {
        this.min = min;
        return this;
    }


    public InformationProgress setMax( int max ) {
        this.max = max;
        return this;
    }


    public void updateProgress( int value ) {
        this.value = value;
        InformationManager.getInstance().notify( this );
    }

}
