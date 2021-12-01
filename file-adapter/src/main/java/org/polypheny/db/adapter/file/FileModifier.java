/*
 * Copyright 2019-2021 The Polypheny Project
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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.file.FileAlg.FileImplementor.Operation;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.DateString;
import org.polypheny.db.util.FileInputHandle;
import org.polypheny.db.util.TimeString;
import org.polypheny.db.util.TimestampString;


public class FileModifier extends FileEnumerator {

    private final Object[] insertValues;
    private boolean inserted = false;


    public FileModifier(
            final Operation operation,
            final String rootPath,
            final Long partitionId,
            final Long[] columnIds,
            final PolyType[] columnTypes,
            final List<Long> pkIds,
            final DataContext dataContext,
            final Object[] insertValues,
            final Condition condition ) {
        super( operation, rootPath, partitionId, columnIds, columnTypes, pkIds, null, dataContext, condition, null );
        this.insertValues = insertValues;
    }


    @Override
    public Object current() {
        return current;
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

                        File newFile = new File( columnFolders.get( i ), getNewFileName( Operation.INSERT, String.valueOf( hash ) ) );
                        if ( value instanceof FileInputHandle ) {
                            if ( newFile.exists() ) {
                                if ( !newFile.delete() ) {
                                    throw new RuntimeException( "Could not delete file" );
                                }
                            }
                            ((FileInputHandle) value).materializeAsFile( newFile.toPath() );
                            continue;
                        }
                        write( newFile, value );
                    }
                }
                current = Long.valueOf( insertPosition );
                inserted = true;
                return true;
            }
        } catch ( IOException | RuntimeException e ) {
            throw new RuntimeException( e );
        }
    }


    static void write( File newFile, Object value ) throws IOException {
        if ( !newFile.createNewFile() ) {
            throw new RuntimeException( "Primary key conflict! You are trying to insert a row with a primary key that already exists." );
        }
        if ( value instanceof byte[] ) {
            Files.write( newFile.toPath(), (byte[]) value );
        } else if ( value instanceof InputStream ) {
            //see https://attacomsian.com/blog/java-convert-inputstream-to-outputstream
            try ( InputStream is = (InputStream) value; FileOutputStream os = new FileOutputStream( newFile ) ) {
                IOUtils.copyLarge( is, os );
            }
        } else if ( FileHelper.isSqlDateOrTimeOrTS( value ) ) {
            Long l = FileHelper.sqlToLong( value );
            Files.write( newFile.toPath(), l.toString().getBytes( FileStore.CHARSET ) );
        } else if ( value instanceof TimestampString ) {
            Files.write( newFile.toPath(), ("" + ((TimestampString) value).getMillisSinceEpoch()).getBytes( StandardCharsets.UTF_8 ) );
        } else if ( value instanceof DateString ) {
            Files.write( newFile.toPath(), ("" + ((DateString) value).getDaysSinceEpoch()).getBytes( StandardCharsets.UTF_8 ) );
        } else if ( value instanceof TimeString ) {
            Files.write( newFile.toPath(), ("" + ((TimeString) value).getMillisOfDay()).getBytes( StandardCharsets.UTF_8 ) );
        } else {
            String writeString = value.toString();
            Files.write( newFile.toPath(), writeString.getBytes( FileStore.CHARSET ) );
        }
    }


    @Override
    public void reset() {
        //insertPosition = 0;
    }


    @Override
    public void close() {

    }

}
