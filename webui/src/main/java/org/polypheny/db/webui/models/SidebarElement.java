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

package org.polypheny.db.webui.models;


import java.util.ArrayList;
import java.util.List;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.polypheny.db.catalog.logistic.DataModel;


/**
 * Can be used to define data for the left sidebar in the UI.
 * Required for Gson.
 */
@Accessors(chain = true)
public class SidebarElement {

    private final DataModel dataModel;
    private String id;
    private String name;
    @Setter
    private String tableType;
    private String icon;//todo: enum
    private String routerLink;
    @Setter
    private String label;
    private String cssClass;
    private List<SidebarElement> children = new ArrayList<>();


    /**
     * Constructor
     *
     * @param id unique id for the SidebarElement, e.g. of the form "schemaName.tableName.columnName"
     * @param name the name of the SidebarElement that will be displayed in the UI
     * @param dataModel the schema type of the sidebar element, this is nullable for non-database elements
     * @param routerLinkRoot routerLink to the view where the Sidebar is displayed. When clicking on a SidebarElement, the user will be directed to the page "routerLinkRoot/id" (id of the SidebarElement)
     * @param icon class name of the icon that will be displayed left of the id, e.g. "fa fa-table"
     */
    public SidebarElement( final String id, final String name, DataModel dataModel, final String routerLinkRoot, String icon ) {
        this.id = id;
        this.name = name;
        this.dataModel = dataModel;
        if ( !routerLinkRoot.isEmpty() ) {
            this.routerLink = routerLinkRoot + id;
        } else {
            this.routerLink = "";
        }
        this.icon = icon;
    }


    /**
     * Add an ArrayList of SidebarElements as children of this one.
     */
    public SidebarElement addChildren( final List<SidebarElement> children ) {
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
