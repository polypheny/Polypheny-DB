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

package org.polypheny.db.adapter.file.source;


import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.file.Condition;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.transaction.Transaction.MultimediaFlavor;
import org.polypheny.db.type.PolyType;


public class QfsEnumerator<E> implements Enumerator<E> {

    private final DataContext dataContext;
    private final List<String> columns;
    private final PolyType[] columnTypes;
    private final Integer[] projectionMapping;
    private final Condition condition;
    private final Iterator<Path> iterator;
    private E current;


    public QfsEnumerator( final DataContext dataContext, final String path, final Long[] columnIds, final Integer[] projectionMapping, final Condition condition ) {
        this.dataContext = dataContext;
        File root = new File( path );

        try {
            this.iterator = Files.walk( root.toPath() ).filter( file -> !file.toFile().isHidden() ).iterator();
        } catch ( IOException e ) {
            throw new RuntimeException( "Unable to query the file system", e );
        }

        List<String> columns = new ArrayList<>();
        List<PolyType> columnTypes = new ArrayList<>();
        this.projectionMapping = projectionMapping;

        if ( condition == null && this.projectionMapping != null ) {
            for ( int projection : this.projectionMapping ) {
                long colId = columnIds[projection];
                CatalogColumn col = Catalog.getInstance().getColumn( colId );
                columns.add( col.name );
                columnTypes.add( col.type );
            }
        } else {
            for ( long colId : columnIds ) {
                CatalogColumn col = Catalog.getInstance().getColumn( colId );
                columns.add( col.name );
                columnTypes.add( col.type );
            }
        }
        this.columns = columns;
        this.columnTypes = columnTypes.toArray( new PolyType[0] );
        this.condition = condition;
    }


    @Override
    public E current() {
        return current;
    }


    @Override
    public boolean moveNext() {
        if ( dataContext.getStatement().getTransaction().getCancelFlag().get() ) {
            return false;
        } else if ( !iterator.hasNext() ) {
            return false;
        }
        Path path = iterator.next();
        Object[] row = getRow( path.toFile() );
        if ( condition != null && !condition.matches( row, columnTypes, dataContext ) ) {
            return moveNext();
        }
        Object[] curr = project( row );
        if ( curr.length == 1 ) {
            current = (E) curr[0];
        } else {
            current = (E) curr;
        }
        return true;
    }


    private Object[] getRow( final File file ) {
        List<Object> row = new ArrayList<>();
        for ( String col : columns ) {
            switch ( col ) {
                case "path":
                    row.add( file.getAbsolutePath() );
                    break;
                case "name":
                    row.add( file.getName() );
                    break;
                case "size":
                    if ( file.isFile() ) {
                        row.add( file.length() );
                    } else {
                        row.add( null );
                    }
                    break;
                case "file":
                    if ( dataContext.getStatement().getTransaction().getFlavor() == MultimediaFlavor.DEFAULT ) {
                        if ( file.isFile() ) {
                            try {
                                row.add( Files.readAllBytes( file.toPath() ) );
                            } catch ( IOException e ) {
                                throw new RuntimeException( "Could not return QFS file as a byte array", e );
                            }
                        } else {
                            row.add( null );
                        }
                    } else {
                        row.add( file );
                    }
                    break;
                default:
                    throw new RuntimeException( "The QFS data source has not implemented the column " + col + " yet" );
            }
        }
        return row.toArray( new Object[0] );
    }


    private Object[] project( final Object[] row ) {
        // If there is no condition, the projection has already been performed
        if ( this.projectionMapping == null || condition == null ) {
            return row;
        }
        Object[] out = new Object[this.projectionMapping.length];
        for ( int i = 0; i < this.projectionMapping.length; i++ ) {
            out[i] = row[this.projectionMapping[i]];
        }
        return out;
    }


    @Override
    public void reset() {

    }


    @Override
    public void close() {

    }

}
