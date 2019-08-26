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


/**
 * Information about a column of a table for the header of a table in the UI
 */
public class DbColumn {

    public String name;

    // for both
    public String dataType;//varchar/int/etc

    // for the Data-Table in the UI
    public SortState sort;
    public String filter;

    // for editing columns
    public boolean primary;
    public boolean nullable;
    public Integer maxLength;
    public String defaultValue;


    public DbColumn( final String name ) {
        this.name = name;
    }


    public DbColumn( final String name, final String dataType, final boolean nullable, final Integer maxLength, final SortState sort, final String filter ) {
        this.name = name;
        this.dataType = dataType;
        this.nullable = nullable;
        if ( dataType.equals( "varchar" ) ) {
            this.maxLength = maxLength;
        }
        this.sort = sort;
        this.filter = filter;
    }


    public DbColumn( final String name, final String dataType, final boolean nullable, final Integer maxLength, final boolean primary, final String defaultValue ) {
        this.name = name;
        this.dataType = dataType;
        this.nullable = nullable;
        this.maxLength = maxLength;
        this.primary = primary;
        this.defaultValue = defaultValue;
    }

}
