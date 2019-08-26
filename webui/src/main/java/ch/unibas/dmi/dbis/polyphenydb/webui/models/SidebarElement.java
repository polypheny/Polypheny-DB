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

package ch.unibas.dmi.dbis.polyphenydb.webui.models;


import java.util.ArrayList;


/**
 * Can be used to define data for the left sidebar in the UI.
 * Required for Gson
 */
public class SidebarElement {

    private String id;
    private String name;
    private String icon;//todo: enum
    private String routerLink;
    private String cssClass;
    private ArrayList<SidebarElement> children = new ArrayList<>();


    /**
     * Constructor
     *
     * @param id unique id for the SidebarElement, e.g. of the form "schemaName.tableName.columnName"
     * @param name the name of the SidebarElement that will be displayed in the UI
     * @param routerLinkRoot routerLink to the view where the Sidebar is displayed. When clicking on a SidebarElement, the user will be directed to the page "routerLinkRoot/id" (id of the SidebarElement)
     */
    public SidebarElement( final String id, final String name, final String routerLinkRoot ) {
        this.id = id;
        this.name = name;
        if ( !routerLinkRoot.equals( "" ) ) {
            this.routerLink = routerLinkRoot + id;
        } else {
            this.routerLink = "";
        }
    }


    /**
     * Constructor
     *
     * @param id unique id for the SidebarElement, e.g. of the form "schemaName.tableName.columnName"
     * @param name the name of the SidebarElement that will be displayed in the UI
     * @param routerLinkRoot routerLink to the view where the Sidebar is displayed. When clicking on a SidebarElement, the user will be directed to the page "routerLinkRoot/id" (id of the SidebarElement)
     * @param icon class name of the icon that will be displayed left of the id, e.g. "fa fa-table"
     */
    public SidebarElement( final String id, final String name, final String routerLinkRoot, final String icon ) {
        this( id, name, routerLinkRoot );
        this.icon = icon;
    }


    /**
     * Add an ArrayList of SidebarElements as children of this one.
     */
    public SidebarElement addChildren( final ArrayList<SidebarElement> children ) {
        this.children.addAll( children );
        return this;
    }


    /**
     * Add a SidebarElement as a child of this one.
     */
    public SidebarElement addChild( final SidebarElement child ) {
        this.children.add( child );
        return this;
    }


    /**
     * Set the routerLink of this SidebarElement.
     */
    public SidebarElement setRouterLink( final String routerLink ) {
        this.routerLink = routerLink;
        return this;
    }


    /**
     * Set a css class for this SidebarElement
     */
    public SidebarElement setCssClass( final String cssClass ) {
        this.cssClass = cssClass;
        return this;
    }

}
