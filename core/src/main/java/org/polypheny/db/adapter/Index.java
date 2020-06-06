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
import org.polypheny.db.QueryProcessor;
import org.polypheny.db.catalog.Catalog.IndexType;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.jdbc.PolyphenyDbSignature;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.transaction.Transaction;


public abstract class Index {

    // The logical schema of the table this index is for
    @Getter
    protected CatalogSchema schema;

    // The logical table this index is for
    @Getter
    protected CatalogTable table;

    // The list of columns over which the index was created
    protected List<String> columns;
    @Getter
    protected IndexType type;

    // Whether this is an unique index
    @Getter
    protected boolean unique;


    public List<String> getColumns() {
        return ImmutableList.copyOf( this.columns );
    }


    /**
     * Trigger an index rebuild, e.g. at crash recovery.
     */
    public void rebuild( Context context ) {
        final Transaction transaction = context.getTransaction();
        // Prepare query
        final RelBuilder builder = RelBuilder.create( transaction );
        final RelNode scan = builder
                .scan( table.toString() )
                .project( columns.stream().map( builder::field ).collect( Collectors.toList() ) )
                .build();
        final QueryProcessor processor = context.getTransaction().getQueryProcessor();
        final PolyphenyDbSignature signature = processor.prepareQuery( RelRoot.of( scan, SqlKind.SELECT ) );
        // Execute query
        final Iterable<Object> enumerable = signature.enumerable( transaction.getDataContext() );
        final Iterator<Object> iterator = enumerable.iterator();
        // TODO(s3lph): Collecting the entire result set in memory may not be preferrable for large tables, use the Avatica Cursor API instead?
        final List<List<Object>> rows = MetaImpl.collect( signature.cursorFactory, iterator, new ArrayList<>() );
        // Rebuild index
        this.clear();
//        this.insertAll( (List<List<RexLiteral>>) rows );
    }


    /**
     * The default implementation is simply a loop over the iterable.
     * Implementations may choose to override this method.
     */
    public void insertAll( final Iterable<List<RexLiteral>> values ) {
        for ( final List<RexLiteral> value : values ) {
            this.insert( value );
        }
    }


    /**
     * Removes all entries from the index.
     */
    protected abstract void clear();

    public abstract void insert( final List<RexLiteral> values );

    public abstract void delete( final List<RexLiteral> values );

    public abstract boolean contains( final List<RexLiteral> value );

    public abstract boolean containsAny( final Set<List<RexLiteral>> values );

    public abstract boolean containsAll( final Set<List<RexLiteral>> values );

}
