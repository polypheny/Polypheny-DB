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


import java.util.UUID;


/**
 * An Information object containing html code that will be rendered in the UI.
 */
public class InformationHtml extends Information {

    private String html;


    /**
     * Constructor
     *
     * @param group The group this information element belongs to
     * @param html The html code
     */
    public InformationHtml( final InformationGroup group, final String html ) {
        this( UUID.randomUUID().toString(), group.getId(), html );
    }


    /**
     * Constructor
     *
     * @param groupId The id of the group this information element belongs to
     * @param html The html code
     */
    public InformationHtml( final String groupId, final String html ) {
        this( UUID.randomUUID().toString(), groupId, html );
    }


    /**
     * Constructor
     *
     * @param id The id of this element
     * @param groupId The id of the group this information element belongs to
     * @param html The html code
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
