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
 * can be used to define data for the left sidebar in the UI
 * needed for Gson
 */
public class SidebarElement {

    private String id;
    private String name;
    private String icon;//todo: enum
    private String routerLink;
    private ArrayList<SidebarElement> children = new ArrayList<>();


    public SidebarElement( final String id, final String name ) {
        this.id = id;
        this.name = name;
        this.routerLink = "/views/data-table/" + id;
    }


    public SidebarElement( final String id, final String name, final String icon ) {
        this( id, name );
        this.icon = icon;
    }


    public SidebarElement addChildren( final ArrayList<SidebarElement> children ) {
        this.children.addAll( children );
        return this;
    }


    public SidebarElement addChild( final SidebarElement child ) {
        this.children.add( child );
        return this;
    }

    public SidebarElement setRouterLink( final String routerLink ) {
        this.routerLink = routerLink;
        return this;
    }
}

