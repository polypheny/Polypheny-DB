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

package org.polypheny.db.adapter;


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.calcite.avatica.MetaImpl;
import org.polypheny.db.catalog.Catalog.IndexType;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.jdbc.PolyphenyDbSignature;
import org.polypheny.db.processing.QueryProcessor;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.core.Values;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.util.Pair;


public abstract class Index {

    @Getter
    protected long id;

    @Getter
    protected String name;

    // The logical schema of the table this index is for
    @Getter
    protected CatalogSchema schema;

    // The logical table this index is for
    @Getter
    protected CatalogTable table;
    // The logical table this index resolves at
    @Getter
    protected CatalogTable targetTable;

    // The list of columns over which the index was created
    protected List<String> columns;
    // The primary key columns the index resolves to
    protected List<String> targetColumns;

    @Getter
    protected IndexType type;

    // Whether this is an unique index
    @Getter
    protected boolean unique;


    public List<String> getColumns() {
        return ImmutableList.copyOf( this.columns );
    }

    public List<String> getTargetColumns() {
        return ImmutableList.copyOf( this.targetColumns );
    }


    /**
     * Trigger an index rebuild, e.g. at crash recovery.
     */
    public void rebuild( final Transaction transaction ) {
        // Prepare query
        final RelBuilder builder = RelBuilder.create( transaction );
        final RexBuilder rb = builder.getRexBuilder();
        List<String> cols = new ArrayList<>( columns );
        if ( table.equals( targetTable ) && !columns.equals( targetColumns )) {
            cols.addAll( targetColumns );
        }
        final RelNode scan = builder
                .scan( table.name )
                .project( columns.stream().map( builder::field ).collect( Collectors.toList() ) )
                .build();
        final QueryProcessor processor = transaction.getQueryProcessor();
        final PolyphenyDbSignature signature = processor.prepareQuery( RelRoot.of( scan, SqlKind.SELECT ) );
        // Execute query
        final Iterable<Object> enumerable = signature.enumerable( transaction.getDataContext() );
        final Iterator<Object> iterator = enumerable.iterator();
        // TODO(s3lph): Collecting the entire result set in memory may not be preferrable for large tables, use the Avatica Cursor API instead?
        final List<List<Object>> rows = MetaImpl.collect( signature.cursorFactory, iterator, new ArrayList<>() );
        final List<Pair<List<Object>, List<Object>>> kv = new ArrayList<>( rows.size() );
        for (final List<Object> row : rows) {
            if (row.size() > columns.size()) {
                kv.add( new Pair<>( row.subList( 0, columns.size() ), row.subList( columns.size(), columns.size() + targetColumns.size() ) ) );
            } else {
                kv.add( new Pair<>( row, row ) );
            }
        }
        // Rebuild index
        this.clear();
        this.insertAll( kv );
    }


    /**
     * The default implementation is simply a loop over the iterable.
     * Implementations may choose to override this method.
     */
    public void insertAll( final Iterable<Pair<List<Object>, List<Object>>> values ) {
        for ( final Pair<List<Object>, List<Object>> row : values ) {
            this.insert( row.getKey(), row.getValue() );
        }
    }


    /**
     * The default implementation is simply a loop over the iterable.
     * Implementations may choose to override this method.
     */
    public void deleteAll( final Iterable<List<Object>> values ) {
        for ( final List<Object> value : values ) {
            this.delete( value );
        }
    }


    /**
     * The default implementation is simply a loop over the iterable.
     * Implementations may choose to override this method.
     */
    public void reverseDeleteAll( final Iterable<List<Object>> values ) {
        for ( final List<Object> value : values ) {
            this.reverseDelete( value );
        }
    }


    /**
     * Removes all entries from the index.
     */
    protected abstract void clear();

    public abstract void insert( final List<Object> key, final List<Object> value );

    public abstract void delete( final List<Object> values );

    public abstract void reverseDelete( final List<Object> values );

    public abstract boolean contains( final List<Object> value );

    public abstract boolean containsAny( final Set<List<Object>> values );

    public abstract boolean containsAll( final Set<List<Object>> values );

    public abstract Values getAsValues( RelBuilder builder, RelDataType rowType );

}
