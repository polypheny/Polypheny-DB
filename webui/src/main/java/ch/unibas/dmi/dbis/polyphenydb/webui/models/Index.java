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


import java.util.StringJoiner;


/**
 * Schema for the index of a table
 */
public class Index {

    String schema;
    String table;
    String name;
    String method;
    String[] columns;

    public Index( final String schema, final String table, final String name, final String method, final String[] columns ) {
        this.schema = schema;
        this.table = table;
        this.name = name;
        this.method = method;
        this.columns = columns;
    }

    /**
     * Generate the query to create an index
     */
    public String create() {
        StringJoiner joiner = new StringJoiner( "," );
        for ( String col : columns ) {
            joiner.add( col );
        }
        return String.format( "CREATE INDEX %s ON %s.%s USING %s (%s)", this.name, this.schema, this.table, this.method, joiner.toString() );
    }


    /**
     * Generate the query to drop an index
     */
    public String drop() {
        return String.format( "DROP INDEX %s", this.name );
    }


    /**
     * Convert index to a row to display in the UI
     */
    public String[] asRow() {
        String[] row = new String[3];
        row[0] = this.name;
        row[1] = this.method;
        row[2] = String.join( ",", this.columns );
        return row;
    }

}
