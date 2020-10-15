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
import java.util.concurrent.atomic.AtomicBoolean;
import org.polypheny.db.type.PolyType;


public class FileModifier<E> extends FileEnumerator<E> {

    private final Long[] columnIds;
    private final Object[] insertValues;
    private int insertPosition = 0;
    private final File rootFile;

    public FileModifier( String rootPath, Long[] columnIds, PolyType[] columnTypes, AtomicBoolean cancelFlag, Object[] insertValues ) {
        super( rootPath, columnIds, columnTypes, cancelFlag );
        this.insertValues = insertValues;
        this.rootFile = new File( rootPath );
        this.columnIds = columnIds;
    }

    @Override
    public E current() {
        return current;
    }

    @Override
    public boolean moveNext() {
        try {
            outer:
            for ( ; ; ) {
                if ( cancelFlag.get() || insertPosition >= insertValues.length ) {
                    return false;
                }
                Object[] currentRow = (Object[]) insertValues[insertPosition];
                int hash = Arrays.hashCode( currentRow );
                for ( int i = 0; i < currentRow.length; i++ ) {
                    Object value = currentRow[i];
                    if ( value == null ) {
                        continue;
                    }
                    File columnFolder = new File( rootFile, FileStore.getPhysicalColumnName( columnIds[i] ) );
                    File newFile = new File( columnFolder, String.valueOf( hash ) );
                    if ( !newFile.createNewFile() ) {
                        throw new RuntimeException( "Primary key conflict! You are trying to insert a row that already exists." );
                    }
                    Files.write( newFile.toPath(), value.toString().getBytes( encoding ) );
                }
                insertPosition++;
                current = (E) Long.valueOf( insertPosition );
                return true;
            }
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }
    }

    @Override
    public void reset() {
        insertPosition = 0;
    }

    @Override
    public void close() {

    }
}
