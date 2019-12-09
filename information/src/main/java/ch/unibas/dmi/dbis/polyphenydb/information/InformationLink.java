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
 * An Information object containing a link to a subpage of the UI.
 */
public class InformationLink extends Information {

    private String label;
    private String[] routerLink;


    /**
     * Constructor
     *
     * @param group The group this object belongs to
     * @param label Name of the link
     * @param routerLink Link to a subpage of the UI
     */
    public InformationLink( final InformationGroup group, final String label, final String... routerLink ) {
        this( group.getId(), label, routerLink );
    }


    /**
     * Constructor
     *
     * @param groupId Id of the group this object belongs to
     * @param label Name of the link
     * @param routerLink Link to a subpage of the UI
     */
    public InformationLink( final String groupId, final String label, final String... routerLink ) {
        this( UUID.randomUUID().toString(), groupId, label, routerLink );
    }


    /**
     * Constructor
     *
     * @param id Id of this Information object
     * @param groupId Id of the group this object belongs to
     * @param label Name of the link
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
     * @param label The name of the link when it is displayed
     * @param routerLink Angular route to another subpage of the WebUI
     */
    public void updateLink( final String label, final String... routerLink ) {
        this.label = label;
        this.routerLink = routerLink;
        notifyManager();
    }

}
