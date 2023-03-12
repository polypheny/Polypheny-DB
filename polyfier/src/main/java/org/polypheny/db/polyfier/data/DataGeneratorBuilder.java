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

package org.polypheny.db.polyfier.data;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.builder.Builder;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.polyfier.core.PolyfierException;
import org.polypheny.db.util.Pair;

@Slf4j
@Getter(AccessLevel.PRIVATE)
public class DataGeneratorBuilder implements Builder<DataGenerator> {

    private final HashMap<Long, ColumnOption> options;
    private final HashMap<Long, Integer> sizes;
    private final long seed;
    private final int buffer;



    public DataGeneratorBuilder( long seed, int buffer ) {

        this.options = new HashMap<>();
        this.sizes = new HashMap<>();
        this.seed = seed;
        this.buffer = buffer;
    }

    @SuppressWarnings( "unused" )
    public DataGeneratorBuilder addCatalogTable( CatalogTable catalogTable, int size ) {
        catalogTable.fieldIds.stream().map( ColumnOption::new ).forEach( o -> options.put( o.getCatalogColumn(), o ) );
        catalogTable.fieldIds.forEach(c -> getOptions().get( c ).setType( Catalog.getInstance().getColumn( c ).type ) );
        sizes.put( catalogTable.id, size );
        return this;
    }

    @SuppressWarnings( "unused" )
    public DataGeneratorBuilder addDateRangeOption( CatalogColumn column, String start, String end ) {
        try {
            getOptions().get( column.id ).setDateRange( start, end );
            getOptions().get( column.id ).setModified( true );
        } catch ( ParseException e ) {
            throw new PolyfierException( "Range could not be parsed", e );
        }
        return this;
    }

    @SuppressWarnings( "unused" )
    public DataGeneratorBuilder addTimestampRangeOption( CatalogColumn column, String start, String end ) {
        try {
            getOptions().get( column.id ).setTimestampRange( start, end );
            getOptions().get( column.id ).setModified( true );
        } catch ( ParseException e ) {
            throw new PolyfierException( "Range could not be parsed", e );
        }
        return this;
    }

    @SuppressWarnings( "unused" )
    public DataGeneratorBuilder addTimestampZRangeOption( CatalogColumn column, String start, String end ) {
        try {
            getOptions().get( column.id ).setTimestampRangeZ( start, end );
            getOptions().get( column.id ).setModified( true );
        } catch ( ParseException e ) {
            throw new PolyfierException( "Range could not be parsed", e );
        }
        return this;
    }

    @SuppressWarnings( "unused" )
    public DataGeneratorBuilder addTimeRangeOption( CatalogColumn column, String start, String end ) {
        try {
            getOptions().get( column.id ).setTimeRange( start, end );
            getOptions().get( column.id ).setModified( true );
        } catch ( ParseException e ) {
            throw new PolyfierException( "Range could not be parsed", e );
        }
        return this;
    }

    @SuppressWarnings( "unused" )
    public DataGeneratorBuilder addTimeZRangeOption( CatalogColumn column, String start, String end ) {
        try {
            getOptions().get( column.id ).setTimeRangeZ( start, end );
            getOptions().get( column.id ).setModified( true );
        } catch ( ParseException e ) {
            throw new PolyfierException( "Range could not be parsed", e );
        }
        return this;
    }

    @SuppressWarnings( "unused" )
    public DataGeneratorBuilder addIntRangeOption( CatalogColumn column, int start, int end ) {
        getOptions().get( column.id ).setIntRange( new Pair<>( start, end ) );
        getOptions().get( column.id ).setModified( true );
        return this;
    }

    @SuppressWarnings( "unused" )
    public DataGeneratorBuilder addFloatRangeOption( CatalogColumn column, float start, float end ) {
        getOptions().get( column.id ).setFloatRange( new Pair<>( start, end ) );
        getOptions().get( column.id ).setModified( true );
        return this;
    }

    @SuppressWarnings( "unused" )
    public DataGeneratorBuilder addDoubleRangeOption( CatalogColumn column, double start, double end ) {
        getOptions().get( column.id ).setDoubleRange( new Pair<>( start, end ) );
        getOptions().get( column.id ).setModified( true );
        return this;
    }

    @SuppressWarnings( "unused" )
    public DataGeneratorBuilder addLengthOption( CatalogColumn column, int length ) {
        getOptions().get( column.id ).setLength( length );
        getOptions().get( column.id ).setModified( true );
        return this;
    }

    @SuppressWarnings( "unused" )
    public DataGeneratorBuilder addUniformValues( CatalogColumn column, List<Object> objects ) {
        getOptions().get( column.id ).setUniformValues( objects );
        getOptions().get( column.id ).setModified( true );
        return this;
    }

    @SuppressWarnings( "unused" )
    public DataGeneratorBuilder addNullable( CatalogColumn column, float probability ) {
        getOptions().get( column.id ).setNullProbability( probability );
        getOptions().get( column.id ).setModified( true );
        return this;
    }


    private void updateOptionsWithKeys() {
        Catalog catalog = Catalog.getInstance();
        getSizes().keySet().forEach( table -> {
            catalog.getForeignKeys( table ).forEach( k -> {
                for ( int i = 0; i < k.referencedKeyColumnIds.size(); i++ ) {
                    Long referencing = k.columnIds.get( i );
                    Long referenced = k.referencedKeyColumnIds.get( i );
                    // Only consider references within the list of tables generating data for
                    if ( getOptions().containsKey( referenced ) ) {
                        getOptions().get( referenced ).getReferencedBy().add( referencing );
                        getOptions().get( referencing ).setReferencedColumn( referenced );
                    }
                }
            } );
            for ( Long primaryKeyColumnId : catalog.getPrimaryKey( catalog.getTable( table ).primaryKey ).columnIds ) {
                getOptions().get( primaryKeyColumnId ).setUnique( true );
                getOptions().get( primaryKeyColumnId ).setModified( true );
            }
        } );
    }

    private DataRecordGenerator getRecordGenerator() {
        Catalog catalog = Catalog.getInstance();
        Random random = new Random( seed );
        return new DataRecordGenerator(
                getOptions(),
                getSizes(),
                new ArrayList<>( getOptions().keySet() ).stream().collect( Collectors.toMap( c -> c, c -> getSizes().get(
                        catalog.getColumn( c ).tableId ) ) ),
                getOptions().keySet().stream().collect( Collectors.toMap( v -> v, v -> random.nextLong() ) )
        );
    }

    @Override
    public DataGenerator build() {
        updateOptionsWithKeys();
        return new DataGenerator( getRecordGenerator(), getBuffer() );
    }

}
