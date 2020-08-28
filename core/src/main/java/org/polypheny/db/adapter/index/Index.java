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

package org.polypheny.db.adapter.index;


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
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.transaction.PolyXid;
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

    // The list of columns over which the index was created
    protected List<String> columns;
    // The primary key columns the index resolves to
    protected List<String> targetColumns;

    public abstract IndexType getType();

    public abstract boolean isUnique();


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
        List<String> cols = new ArrayList<>( columns );
        if (!columns.equals( targetColumns )) {
            cols.addAll( targetColumns );
        }
        final RelNode scan = builder
                .scan( table.name )
                .project( cols.stream().map( builder::field ).collect( Collectors.toList() ) )
                .build();
        final QueryProcessor processor = transaction.getQueryProcessor();
        final PolyphenyDbSignature signature = processor.prepareQuery( RelRoot.of( scan, SqlKind.SELECT ) );
        // Execute query
        final Iterable<Object> enumerable = signature.enumerable( transaction.getDataContext() );
        final Iterator<Object> iterator = enumerable.iterator();
        final List<List<Object>> rows = MetaImpl.collect( signature.cursorFactory, iterator, new ArrayList<>() );
        final List<Pair<List<Object>, List<Object>>> kv = new ArrayList<>( rows.size() );
        for (final List<Object> row : rows) {
            if (row.size() > columns.size()) {
                kv.add( new Pair<>( row.subList( 0, columns.size() ), row.subList( columns.size(), columns.size() + targetColumns.size() ) ) );
            } else {
                // Columns and target columns are identical, i.e. this is a primary key index
                kv.add( new Pair<>( row, row ) );
            }
        }
        // Rebuild index
        this.clear();
        this.insertAll( kv );
    }

    abstract void commit( PolyXid xid );

    abstract void rollback( PolyXid xid );

    /**
     * The default implementation is simply a loop over the iterable.
     * Implementations may choose to override this method.
     */
    public void insertAll( PolyXid xid, final Iterable<Pair<List<Object>, List<Object>>> values ) {
        for ( final Pair<List<Object>, List<Object>> row : values ) {
            this.insert( xid, row.getKey(), row.getValue() );
        }
    }

    /**
     * The default implementation is simply a loop over the iterable.
     * Implementations may choose to override this method.
     */
    void insertAll( final Iterable<Pair<List<Object>, List<Object>>> values ) {
        for ( final Pair<List<Object>, List<Object>> row : values ) {
            this.insert( row.getKey(), row.getValue() );
        }
    }


    /**
     * The default implementation is simply a loop over the iterable.
     * Implementations may choose to override this method.
     */
    public void deleteAll( PolyXid xid, final Iterable<List<Object>> values ) {
        for ( final List<Object> value : values ) {
            this.delete( xid, value );
        }
    }


    /**
     * The default implementation is simply a loop over the iterable.
     * Implementations may choose to override this method.
     */
    public void deleteAllPrimary( PolyXid xid, final Iterable<Pair<List<Object>, List<Object>>> values ) {
        for ( final Pair<List<Object>, List<Object>> value : values ) {
            this.deletePrimary( xid, value.left, value.right );
        }
    }


    /**
     * The default implementation is simply a loop over the iterable.
     * Implementations may choose to override this method.
     */
    void deleteAll( final Iterable<List<Object>> values ) {
        for ( final List<Object> value : values ) {
            this.delete( value );
        }
    }


    /**
     * The default implementation is simply a loop over the iterable.
     * Implementations may choose to override this method.
     */
    void deleteAllPrimary( final Iterable<Pair<List<Object>, List<Object>>> values ) {
        for ( final Pair<List<Object>, List<Object>> value : values ) {
            this.deletePrimary( value.left, value.right );
        }
    }


    /**
     * Removes all entries from the index.
     */
    protected abstract void clear();

    public abstract void insert( final PolyXid xid, final List<Object> key, final List<Object> value );

    abstract void insert( final List<Object> key, final List<Object> value );

    public abstract void delete( final PolyXid xid, final List<Object> values );

    abstract void deletePrimary( final PolyXid xid, final List<Object> key, final List<Object> primary );

    abstract void delete( final List<Object> values );

    abstract void deletePrimary( final List<Object> key, final List<Object> primary );

    public abstract boolean contains( final PolyXid xid, final List<Object> value );

    public abstract boolean containsAny( final PolyXid xid, final Set<List<Object>> values );

    public abstract boolean containsAll( final PolyXid xid, final Set<List<Object>> values );

    public abstract Values getAsValues( final PolyXid xid, RelBuilder builder, RelDataType rowType );

    interface IndexFactory {

        boolean isUnique();

        IndexType getType();

        Index create(
                final long id,
                final String name,
                final CatalogSchema schema,
                final CatalogTable table,
                final List<String> columns,
                final List<String> targetColumns);

    }

}
