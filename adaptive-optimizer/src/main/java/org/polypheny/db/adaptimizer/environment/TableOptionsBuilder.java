/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.adaptimizer.environment;

import java.text.ParseException;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.lang3.builder.Builder;
import org.polypheny.db.adaptimizer.except.TestDataGenerationException;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.util.Pair;

public class TableOptionsBuilder implements Builder<DataTableOptionTemplate> {
    CatalogTable catalogTable;
    List<CatalogColumn> columns;
    List<DataColumnOption> columnOptions;

    int size;

    public TableOptionsBuilder( Catalog catalog, CatalogTable catalogTable ) {
        this.catalogTable = catalogTable;
        this.columns = catalog.getColumns( catalogTable.id );
        columnOptions = new LinkedList<>();
        this.columns.forEach(c -> columnOptions.add( null ) );
    }

    private DataColumnOption getColumnOptionInstance( String columnName ) {
        for ( int i = 0; i < this.columns.size(); i++ ) {
            if ( columnName.equals( this.columns.get( i ).name ) ) {
                DataColumnOption option = this.columnOptions.get( i );
                if ( option == null ) {
                    option = new DataColumnOption();
                    this.columnOptions.set( i, option );
                }
                return option;
            }
        }
        throw new TestDataGenerationException( "No such column in table.", new IllegalArgumentException() );
    }

    public TableOptionsBuilder addDateRangeOption( String columnName, String start, String end ) {
        try {
            getColumnOptionInstance( columnName ).setDateRange( start, end );
        } catch ( ParseException e ) {
            throw new TestDataGenerationException( "Range could not be parsed", e );
        }
        return this;
    }

    public TableOptionsBuilder addTimestampRangeOption( String columnName, String start, String end ) {
        try {
            getColumnOptionInstance( columnName ).setTimestampRange( start, end );
        } catch ( ParseException e ) {
            throw new TestDataGenerationException( "Range could not be parsed", e );
        }
        return this;
    }

    public TableOptionsBuilder addTimestampZRangeOption( String columnName, String start, String end ) {
        try {
            getColumnOptionInstance( columnName ).setTimestampRangeZ( start, end );
        } catch ( ParseException e ) {
            throw new TestDataGenerationException( "Range could not be parsed", e );
        }
        return this;
    }

    public TableOptionsBuilder addTimeRangeOption( String columnName, String start, String end ) {
        try {
            getColumnOptionInstance( columnName ).setTimeRange( start, end );
        } catch ( ParseException e ) {
            throw new TestDataGenerationException( "Range could not be parsed", e );
        }
        return this;
    }

    public TableOptionsBuilder addTimeZRangeOption( String columnName, String start, String end ) {
        try {
            getColumnOptionInstance( columnName ).setTimeRangeZ( start, end );
        } catch ( ParseException e ) {
            throw new TestDataGenerationException( "Range could not be parsed", e );
        }
        return this;
    }

    public TableOptionsBuilder addIntRangeOption( String columnName, int start, int end ) {
        getColumnOptionInstance( columnName ).setIntRange( new Pair<>( start, end ) );
        return this;
    }

    public TableOptionsBuilder addFloatRangeOption( String columnName, float start, float end ) {
        getColumnOptionInstance( columnName ).setFloatRange( new Pair<>( start, end ) );
        return this;
    }

    public TableOptionsBuilder addDoubleRangeOption( String columnName, double start, double end ) {
        getColumnOptionInstance( columnName ).setDoubleRange( new Pair<>( start, end ) );
        return this;
    }

    public TableOptionsBuilder addLengthOption( String columnName, int length ) {
        getColumnOptionInstance( columnName ).setLength( length );
        return this;
    }

    public TableOptionsBuilder addUniformValues( String columnName, List<Object> objects ) {
        getColumnOptionInstance( columnName ).setUniformValues( objects );
        return this;
    }

    public TableOptionsBuilder addNullable( String columnName, float probability ) {
        getColumnOptionInstance( columnName ).setNullProbability( probability );
        return this;
    }

    public TableOptionsBuilder setSize( int size ) {
        this.size = size;
        return this;
    }


    @Override
    public DataTableOptionTemplate build() {
        return new DataTableOptionTemplate(
                this.catalogTable,
                this.columns,
                this.columnOptions,
                this.size
        );
    }

}
