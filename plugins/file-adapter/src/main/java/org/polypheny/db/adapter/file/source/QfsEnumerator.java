/*
 * Copyright 2019-2024 The Polypheny Project
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
import org.polypheny.db.adapter.file.FileTranslatableEntity;
import org.polypheny.db.adapter.file.Value;
import org.polypheny.db.adapter.file.Value.InputValue;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.transaction.Transaction.MultimediaFlavor;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;


public class QfsEnumerator implements Enumerator<PolyValue[]> {

    private final DataContext dataContext;
    private final List<String> columns;
    private final List<AlgDataTypeField> columnTypes;
    private final List<Value> projectionMapping;
    private final Condition condition;
    private final Iterator<Path> iterator;
    private PolyValue[] current;


    public QfsEnumerator( FileTranslatableEntity entity, final DataContext dataContext, final String path, final Long[] columnIds, final List<Value> projectionMapping, final Condition condition ) {
        this.dataContext = dataContext;
        File root = new File( path );

        try {
            this.iterator = Files.walk( root.toPath() ).filter( file -> !file.toFile().isHidden() ).iterator();
        } catch ( IOException e ) {
            throw new GenericRuntimeException( "Unable to query the file system", e );
        }

        List<String> columns = new ArrayList<>();
        List<AlgDataTypeField> columnTypes = new ArrayList<>();
        this.projectionMapping = projectionMapping;

        if ( condition == null && this.projectionMapping != null ) {
            for ( Value projection : this.projectionMapping ) {
                long colId = columnIds[projection.getValue( List.of( current ), dataContext, 0 ).asNumber().intValue()];
                entity.getTupleType().getFields().stream().filter( col -> col.getId() == colId ).findFirst().ifPresent( col -> {
                    columns.add( col.getName() );
                    columnTypes.add( col );
                } );
            }
        } else {
            for ( long colId : columnIds ) {
                entity.getTupleType().getFields().stream().filter( col -> col.getId() == colId ).findFirst().ifPresent( col -> {
                    columns.add( col.getName() );
                    columnTypes.add( col );
                } );
            }
        }
        this.columns = columns;
        this.columnTypes = columnTypes;
        this.condition = condition;
    }


    @Override
    public PolyValue[] current() {
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
        List<PolyValue> row = Pair.zip( List.of( getRow( path.toFile() ) ), columnTypes ).stream().map( p -> PolyTypeUtil.stringToObject( p.left.toString(), p.right ) ).toList();
        if ( condition != null && !condition.matches( row, columnTypes, dataContext ) ) {
            return moveNext();
        }
        List<PolyValue> curr = project( row );
        current = curr.toArray( new PolyValue[0] );
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
                                throw new GenericRuntimeException( "Could not return QFS file as a byte array", e );
                            }
                        } else {
                            row.add( null );
                        }
                    } else {
                        row.add( file );
                    }
                    break;
                default:
                    throw new GenericRuntimeException( "The QFS data source has not implemented the column " + col + " yet" );
            }
        }
        return row.toArray( new Object[0] );
    }


    private List<PolyValue> project( final List<PolyValue> row ) {
        // If there is no condition, the projection has already been performed
        if ( this.projectionMapping == null || condition == null ) {
            return row;
        }
        List<PolyValue> out = new ArrayList<>( this.projectionMapping.size() );
        for ( int i = 0; i < this.projectionMapping.size(); i++ ) {
            out.set( i, row.get( ((InputValue) this.projectionMapping.get( i )).getIndex() ) );
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
