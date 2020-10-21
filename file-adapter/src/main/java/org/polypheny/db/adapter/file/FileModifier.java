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

package org.polypheny.db.adapter.file;


import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.file.FileRel.FileImplementor.Operation;
import org.polypheny.db.adapter.file.rel.FileFilter;
import org.polypheny.db.type.PolyType;


public class FileModifier<E> extends FileEnumerator<E> {

    private final Long[] columnIds;
    private final Object[] insertValues;
    private final File rootFile;
    private boolean inserted = false;
    final Integer[] pkMapping;

    public FileModifier( final Operation operation,
            final String rootPath,
            final Long[] columnIds,
            final PolyType[] columnTypes,
            final List<Long> pkIds,
            final DataContext dataContext,
            final Object[] insertValues,
            final FileFilter.Condition condition ) {
        //todo projectionMapping
        super( operation, rootPath, columnIds, columnTypes, null, dataContext, condition );
        this.insertValues = insertValues;
        this.rootFile = new File( rootPath );
        this.columnIds = columnIds;
        Integer[] pkMapping = new Integer[pkIds.size()];
        int i = 0;
        List<Long> colIdsAsList = Arrays.asList( columnIds.clone() );
        for ( Long pkId : pkIds ) {
            pkMapping[i] = colIdsAsList.indexOf( pkId );
            i++;
        }
        this.pkMapping = pkMapping;
    }

    @Override
    public E current() {
        return current;
    }

    /**
     * First call during an insert:
     * insert all data, set current to the insertCount, return true
     * Second call:
     * return false
     * see {@link org.polypheny.db.webui.Crud#executeSqlUpdate}
     */
    @Override
    public boolean moveNext() {
        try {
            outer:
            for ( ; ; ) {
                if ( dataContext.getStatement().getTransaction().getCancelFlag().get() || inserted ) {
                    return false;
                }
                int insertPosition;
                for ( insertPosition = 0; insertPosition < insertValues.length; insertPosition++ ) {
                    Object[] currentRow = (Object[]) insertValues[insertPosition];
                    int hash = hashRow( currentRow );
                    for ( int i = 0; i < currentRow.length; i++ ) {
                        Object value = currentRow[i];
                        if ( value == null ) {
                            continue;
                        }
                        File columnFolder = new File( rootFile, FileStore.getPhysicalColumnName( columnIds[i] ) );
                        File newFile = new File( columnFolder, String.valueOf( hash ) );
                        //don't throw for now
                        /*if ( !newFile.createNewFile() ) {
                            throw new RuntimeException( "Primary key conflict! You are trying to insert a row that already exists." );
                        }*/
                        //todo check condition
                        Files.write( newFile.toPath(), value.toString().getBytes( encoding ) );
                    }
                }
                current = (E) new Long( insertPosition );
                inserted = true;
                return true;
            }
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }
    }

    @Override
    public void reset() {
        //insertPosition = 0;
    }

    @Override
    public void close() {

    }

    /**
     * Hash only the elements of a row that are part of the primary key
     */
    private int hashRow( final Object[] row ) {
        Object[] toHash = new Object[pkMapping.length];
        for ( int i = 0; i < pkMapping.length; i++ ) {
            toHash[i] = row[pkMapping[i]];
        }
        return Arrays.hashCode( toHash );
    }
}
