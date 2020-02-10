/*
 * Copyright 2019-2020 The Polypheny Project
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

package ch.unibas.dmi.dbis.polyphenydb.webui.models;


import java.util.ArrayList;


/**
 * Can be used to define data for the left sidebar in the UI.
 * Required for Gson.
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
