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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * an InformationGroup contains multiple Information object that will be rendered together in the UI
 */
public class InformationGroup {

    /**
     * unique id for an InformationGroup
     */
    private final String id;


    /**
     * the id of the page this group belongs to
     */
    private final String pageId;


    /**
     * the color of this group
     * can be set for the UI
     */
    private GroupColor color;


    /**
     * groups with lower uiOrder will be rendered first, the groups with higher order, then groups with no uiOrder (0)
     */
    private int uiOrder;


    /**
     * a Map of Information objects that belong to this group
     */
    private final ConcurrentMap<String, Information> list = new ConcurrentHashMap<>();


    /**
     * constructor
     *
     * @param id id of this group
     * @param pageId id of the page this group belongs to
     */
    public InformationGroup( final String id, final String pageId ) {
        this.id = id;
        this.pageId = pageId;
    }


    /**
     * if you want the group to have a certain color in the UI, you can set it here
     * @param color color for this group
     */
    public InformationGroup setColor( GroupColor color ) {
        this.color = color;
        return this;
    }


    /**
     * the InformationManager will add Information objects to this group that belong to this group
     */
    public void addInformation( Information... infos ) {
        for ( Information i : infos ) {
            if ( !i.getGroup().equals( this.id ) ) {
                throw new InformationRuntimeException( "You are trying to add an information to a group where it does not belong to." );
            }
            this.list.put( i.getId(), i );
        }
    }


    /**
     * groups with lower uiOrder will be rendered first, the groups with higher order, then groups with no uiOrder (0)
     * @param order an int > 0
     */
    public InformationGroup setOrder( int order ) {
        this.uiOrder = order;
        return this;
    }


    /**
     *  return the id for the group
     * @return id of the group
     */
    public String getId() {
        return id;
    }


    /**
     * return the id of the page to which this group belongs to
     * @return the page id of this group
     */
    public String getPageId() {
        return pageId;
    }

}
