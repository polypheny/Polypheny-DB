/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2020 Databases and Information Systems Research Group, University of Basel, Switzerland
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

package ch.unibas.dmi.dbis.polyphenydb.statistic;


import ch.unibas.dmi.dbis.polyphenydb.PolySqlType;
import com.google.gson.annotations.Expose;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;


/**
 * Stores the available statistic data of a specific column
 */
public abstract class StatisticColumn<T extends Comparable<T>> {

    @Expose
    @Getter
    private final String schema;

    @Expose
    @Getter
    private final String table;

    @Expose
    @Getter
    private final String column;

    @Getter
    private final PolySqlType type;

    @Expose
    @Getter
    private final String qualifiedColumnName;

    @Expose
    @Setter
    boolean isFull;

    @Expose
    @Getter
    @Setter
    public List<T> uniqueValues = new ArrayList<>();

    @Expose
    @Getter
    @Setter
    public int count;


    public StatisticColumn( String schema, String table, String column, PolySqlType type ) {
        this.schema = schema.replace( "\\", "" ).replace( "\"", "" );
        this.table = table.replace( "\\", "" ).replace( "\"", "" );
        this.column = column.replace( "\\", "" ).replace( "\"", "" );
        this.type = type;
        this.qualifiedColumnName = this.schema + "." + this.table + "." + this.column;

    }


    StatisticColumn( String[] splitColumn, PolySqlType type ) {
        this( splitColumn[0], splitColumn[1], splitColumn[2], type );
    }


    public String getQualifiedTableName() {
        return this.schema + "." + this.table;
    }


    public abstract void insert( T val );

    public abstract String toString();
}
