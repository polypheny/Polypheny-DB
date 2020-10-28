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
import java.util.List;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.file.FileRel.FileImplementor.Operation;
import org.polypheny.db.type.PolyType;


public class FileModifier<E> extends FileEnumerator<E> {

    private final Object[] insertValues;
    private boolean inserted = false;

    public FileModifier( final Operation operation,
            final String rootPath,
            final Long[] columnIds,
            final PolyType[] columnTypes,
            final List<Long> pkIds,
            final DataContext dataContext,
            final Object[] insertValues,
            final Condition condition ) {
        super( operation, rootPath, columnIds, columnTypes, pkIds, null, dataContext, condition, null );
        this.insertValues = insertValues;
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

                        File newFile = new File( columnFolders.get( i ), getNewFileName( Operation.INSERT, String.valueOf( hash ) ) );
                        if ( !newFile.createNewFile() ) {
                            throw new RuntimeException( "Primary key conflict! You are trying to insert a row with a primary key that already exists." );
                        }
                        String writeString;
                        /*switch ( columnTypes[i] ) {
                            case TIME:
                                if ( value instanceof Time ) {
                                    LocalTime t = ((Time) value).toLocalTime();
                                    writeString = t.format( DateTimeFormatter.ofPattern( DateTimeUtils.TIMESTAMP_FORMAT_STRING ) );
                                } else {
                                    writeString = (String) value;
                                }
                                break;
                            case DATE:
                                if ( value instanceof Date ) {
                                    LocalDate d = ((Date) value).toLocalDate();
                                    LocalDateTime dt = LocalDateTime.of( d, LocalTime.MIN );
                                    writeString = dt.format( DateTimeFormatter.ofPattern( DateTimeUtils.TIMESTAMP_FORMAT_STRING ) );
                                } else {
                                    writeString = (String) value;
                                }
                                break;
                            case TIMESTAMP:
                                if ( value instanceof Timestamp ) {
                                    LocalDateTime dt = ((Timestamp) value).toLocalDateTime();
                                    writeString = dt.format( DateTimeFormatter.ofPattern( DateTimeUtils.TIMESTAMP_FORMAT_STRING ) );
                                } else {
                                    writeString = (String) value;
                                }
                                break;
                            default:
                                writeString = value.toString();

                        }*/
                        writeString = value.toString();
                        Files.write( newFile.toPath(), writeString.getBytes( FileStore.CHARSET ) );
                    }
                }
                current = (E) new Long( insertPosition );
                inserted = true;
                return true;
            }
        } catch ( IOException | RuntimeException e ) {
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
}
