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

package org.polypheny.db.adapter.index;


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.processing.QueryProcessor;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.PolyType;
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


    public abstract String getMethod();

    public abstract boolean isUnique();

    public abstract boolean isPersistent();


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
        Statement statement = transaction.createStatement();

        // Prepare query
        final AlgBuilder builder = AlgBuilder.create( statement );
        List<String> cols = new ArrayList<>( columns );
        if ( !columns.equals( targetColumns ) ) {
            cols.addAll( targetColumns );
        }
        final AlgNode scan = builder
                .scan( ImmutableList.of( table.getNamespaceName(), table.name ) )
                .project( cols.stream().map( builder::field ).collect( Collectors.toList() ) )
                .build();
        final QueryProcessor processor = statement.getQueryProcessor();
        final PolyImplementation result = processor.prepareQuery( AlgRoot.of( scan, Kind.SELECT ), false );
        // Execute query

        final List<List<Object>> rows = result.getRows( statement, 1, false, false, result.getStatement().getMonitoringEvent(), true );
        final List<Pair<List<Object>, List<Object>>> kv = new ArrayList<>( rows.size() );
        for ( final List<Object> row : rows ) {
            if ( row.size() > columns.size() ) {
                kv.add( new Pair<>( row.subList( 0, columns.size() ), row.subList( columns.size(), columns.size() + targetColumns.size() ) ) );
            } else {
                // Columns and target columns are identical, i.e. this is a primary key index
                kv.add( new Pair<>( row, row ) );
            }
        }
        // Rebuild index
        this.clear();
        this.insertAll( kv );
        this.initialize();
    }


    abstract void commit( PolyXid xid );

    abstract void rollback( PolyXid xid );

    abstract public void barrier( PolyXid xid );


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

    abstract void initialize();

    abstract boolean isInitialized();

    public abstract int size();

    public abstract void insert( final PolyXid xid, final List<Object> key, final List<Object> value );

    abstract void insert( final List<Object> key, final List<Object> value );

    public abstract void delete( final PolyXid xid, final List<Object> values );

    abstract void deletePrimary( final PolyXid xid, final List<Object> key, final List<Object> primary );

    abstract void delete( final List<Object> values );

    abstract void deletePrimary( final List<Object> key, final List<Object> primary );

    public abstract boolean contains( final PolyXid xid, final List<Object> value );

    public abstract boolean containsAny( final PolyXid xid, final Iterable<List<Object>> values );

    public abstract boolean containsAll( final PolyXid xid, final Iterable<List<Object>> values );

    public abstract Values getAsValues( final PolyXid xid, AlgBuilder builder, AlgDataType rowType );

    public abstract Values getAsValues( final PolyXid xid, AlgBuilder builder, AlgDataType rowType, final List<Object> key );

    abstract Object getRaw();


    interface IndexFactory {

        boolean canProvide(
                final String method,
                final Boolean unique,
                final Boolean persitent );

        Index create(
                final long id,
                final String name,
                final String method,
                final Boolean unique,
                final Boolean persitent,
                final CatalogSchema schema,
                final CatalogTable table,
                final List<String> columns,
                final List<String> targetColumns );

    }


    /*
     *  Helpers
     */


    protected ImmutableList<RexLiteral> makeRexRow( final AlgDataType rowType, final RexBuilder rexBuilder, final List<Object> tuple ) {
        assert rowType.getFieldCount() == tuple.size();
        List<RexLiteral> row = new ArrayList<>( tuple.size() );
        for ( int i = 0; i < tuple.size(); ++i ) {
            final AlgDataType type = rowType.getFieldList().get( i ).getType();
            final Pair<Comparable, PolyType> converted = RexLiteral.convertType( (Comparable) tuple.get( i ), type );
            row.add( new RexLiteral( converted.left, type, converted.right ) );
        }
        return ImmutableList.copyOf( row );
    }

}
