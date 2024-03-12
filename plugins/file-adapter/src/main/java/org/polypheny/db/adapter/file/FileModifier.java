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

package org.polypheny.db.adapter.file;


import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.file.FileAlg.FileImplementor.Operation;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyLong;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyValue;


public class FileModifier extends FileEnumerator {

    private final List<List<PolyValue>> insertValues;
    private boolean inserted = false;


    public FileModifier(
            final Operation operation,
            final String rootPath,
            final Long partitionId,
            final Long[] columnIds,
            final FileTranslatableEntity entity,
            final List<Long> pkIds,
            final DataContext dataContext,
            final List<List<PolyValue>> insertValues,
            final Condition condition ) {
        super( operation, rootPath, partitionId, columnIds, entity, pkIds, null, dataContext, condition, null );
        this.insertValues = insertValues;
    }


    /**
     * First call during an insert:
     * insert all data, set current to the insertCount, return true
     * Second call:
     * return false
     * see {@code org.polypheny.db.webui.Crud#executeSqlUpdate}
     */
    @Override
    public boolean moveNext() {
        try {
            for ( ; ; ) {
                if ( dataContext.getStatement().getTransaction().getCancelFlag().get() || inserted ) {
                    return false;
                }
                int insertPosition;
                for ( insertPosition = 0; insertPosition < insertValues.size(); insertPosition++ ) {
                    List<PolyValue> currentRow = insertValues.get( insertPosition );
                    int hash = hashRow( currentRow );
                    for ( int i = 0; i < currentRow.size(); i++ ) {
                        PolyValue value = currentRow.get( i );
                        if ( value == null ) {
                            value = PolyNull.NULL;
                        }

                        File newFile = new File( columnFolders.get( i ), getNewFileName( Operation.INSERT, String.valueOf( hash ) ) );
                        if ( !value.isNull() && value.isBlob() && value.asBlob().isHandle() ) {
                            if ( newFile.exists() ) {
                                if ( !newFile.delete() ) {
                                    throw new GenericRuntimeException( "Could not delete file" );
                                }
                            }
                            value.asBlob().getHandle().materializeAsFile( newFile.toPath() );
                            continue;
                        }
                        write( newFile, value );
                    }
                }
                current = new PolyLong[]{ PolyLong.of( insertPosition ) };
                inserted = true;
                return true;
            }
        } catch ( IOException | RuntimeException e ) {
            throw new GenericRuntimeException( e );
        }
    }


    static void write( File newFile, PolyValue value ) throws IOException {
        if ( !newFile.createNewFile() ) {
            throw new GenericRuntimeException( "Primary key conflict! You are trying to insert a row with a primary key that already exists." );
        }
        Files.writeString( newFile.toPath(), value.toTypedJson(), FileStore.CHARSET );
    }


    @Override
    public void reset() {
        //insertPosition = 0;
    }


}
