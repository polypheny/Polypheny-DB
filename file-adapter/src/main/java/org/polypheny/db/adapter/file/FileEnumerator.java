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


import com.google.gson.Gson;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.commons.io.FileUtils;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.file.FileRel.FileImplementor.Operation;
import org.polypheny.db.transaction.Transaction.MultimediaFlavor;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.type.PolyTypeUtil;


@Slf4j
public class FileEnumerator implements Enumerator<Object> {

    Object current;
    final Operation operation;
    final List<File> columnFolders = new ArrayList<>();
    final File[] fileList;
    Integer fileListPosition = 0;
    Long updateDeleteCount = 0L;
    boolean updatedOrDeleted = false;
    final int numOfCols;
    final DataContext dataContext;
    final Condition condition;
    final Integer[] projectionMapping;
    final PolyType[] columnTypes;
    final Gson gson;
    final Map<Integer, Value> updates = new HashMap<>();
    final Integer[] pkMapping;


    /**
     * FileEnumerator
     * When no filter is available, it will only iterate the files in the projected columns
     * If a filter is available, it will iterate over all columns and project each row
     *
     * @param rootPath The rootPath is required to know where the files to iterate are placed
     * @param columnIds Ids of the columns that come from a tableScan. If there is no filter, the enumerator will only iterate over the columns that are specified by the projection
     * @param columnTypes DataTypes of the columns that are given by the {@code columnIds} array
     * @param projectionMapping Mapping on how to project a table. E.g. the array [3,2] means that the row [a,b,c,d,e] will be projected to [c,b].
     * In case of an UPDATE operation, the projectionMapping represents the indexes of the columns that will be updated, e.g. [2,3] means that b and c will be updated.
     * @param dataContext DataContext
     * @param condition Condition that can be {@code null}. The columnReferences in the filter point to the columns coming from the tableScan, not from the projection
     */
    public FileEnumerator( final Operation operation,
            final String rootPath,
            final Long[] columnIds,
            final PolyType[] columnTypes,
            final List<Long> pkIds,
            final Integer[] projectionMapping,
            final DataContext dataContext,
            final Condition condition,
            final Value[] updates ) {

        this.operation = operation;
        if ( operation == Operation.DELETE || operation == Operation.UPDATE ) {
            //fix to make sure current is never null
            current = Long.valueOf( 0L );
        }
        this.dataContext = dataContext;
        this.condition = condition;
        this.projectionMapping = projectionMapping;

        if ( updates != null ) {
            // In case of an UPDATE, the projectionMapping represent the indexes of the columns that will be updated
            for ( Value update : updates ) {
                this.updates.put( update.getColumnReference(), update );
            }
        }

        // pkMapping
        Integer[] pkMapping = new Integer[pkIds.size()];
        int ii = 0;
        List<Long> colIdsAsList = Arrays.asList( columnIds.clone() );
        for ( Long pkId : pkIds ) {
            pkMapping[ii] = colIdsAsList.indexOf( pkId );
            ii++;
        }
        this.pkMapping = pkMapping;

        this.gson = new Gson();
        Long[] columnsToIterate = columnIds;
        // If there is a projection and no filter, it is sufficient to just load the data of the projected columns.
        // If a filter is given, the whole table has to be loaded (because of the column references)
        // If we have an UPDATE operation, the whole table has to be loaded as well, to generate the hashes
        if ( condition == null && projectionMapping != null && operation != Operation.UPDATE ) {
            Long[] projection = new Long[projectionMapping.length];
            PolyType[] projectedTypes = new PolyType[projectionMapping.length];
            for ( int i = 0; i < projectionMapping.length; i++ ) {
                projection[i] = columnIds[projectionMapping[i]];
                projectedTypes[i] = columnTypes[projectionMapping[i]];
            }
            columnsToIterate = projection;
            this.columnTypes = projectedTypes;
        } else {
            this.columnTypes = columnTypes;
        }
        // We want to read data where an insert has been prepared and skip data where a deletion has been prepared.
        @SuppressWarnings("UnstableApiUsage")
        String xidHash = FileStore.SHA.hashString( dataContext.getStatement().getTransaction().getXid().toString(), FileStore.CHARSET ).toString();
        FileFilter fileFilter = file -> !file.isHidden() && (!file.getName().startsWith( "_" ) || file.getName().startsWith( "_ins_" + xidHash ));
        for ( Long colId : columnsToIterate ) {
            File columnFolder = FileStore.getColumnFolder( rootPath, colId );
            columnFolders.add( columnFolder );
        }
        if ( columnsToIterate.length == 1 ) {
            // If we go over a single column, we can iterate it, even if null values are not present as files
            this.fileList = FileStore.getColumnFolder( rootPath, columnsToIterate[0] ).listFiles( fileFilter );
        } else {
            // Iterate over a PK-column, because they are always NOT NULL
            this.fileList = FileStore.getColumnFolder( rootPath, pkIds.get( 0 ) ).listFiles( fileFilter );
        }
        numOfCols = columnFolders.size();
    }


    @Override
    public Object current() {
        return current;
    }


    @Override
    public boolean moveNext() {
        //todo make sure that all requirements of the interface are satisfied
        try {
            outer:
            for ( ; ; ) {
                if ( dataContext.getStatement().getTransaction().getCancelFlag().get() ) {
                    return false;
                }
                if ( fileListPosition >= fileList.length ) {
                    /* Why this is necessary:
                     * First call during a delete:
                     * Delete all data, set current to the deleteCount, return true
                     * Second call: return false
                     */
                    if ( (operation == Operation.DELETE || operation == Operation.UPDATE) && !updatedOrDeleted ) {
                        updatedOrDeleted = true;
                        return true;
                    } else {
                        return false;
                    }
                }
                //if there was a PK lookup
                else if ( fileListPosition < 0 ) {
                    if ( (operation == Operation.DELETE || operation == Operation.UPDATE) && !updatedOrDeleted ) {
                        updatedOrDeleted = true;
                        return true;
                    } else {
                        return false;
                    }
                }
                File currentFile = fileList[fileListPosition];
                Object curr;

                if ( condition != null ) {
                    Object pkLookup = condition.getPKLookup( new HashSet<>( Arrays.asList( pkMapping ) ), columnTypes, numOfCols, dataContext );
                    if ( pkLookup != null ) {
                        int hash = hashRow( pkLookup );
                        File lookupFile = new File( FileStore.SHA.hashString( String.valueOf( hash ), FileStore.CHARSET ).toString() );
                        curr = fileToRow( lookupFile );
                        //set -2, as a flag, so the enumerator knows that it doesn't have to continue
                        //the flag will be increased to -1 in the select/update/delete operation below
                        fileListPosition = -2;
                        //if the first attempt did not match, check if there is an _ins_xid_hash file
                        if ( curr == null ) {
                            lookupFile = new File( getNewFileName( Operation.INSERT, String.valueOf( hash ) ) );
                            curr = fileToRow( lookupFile );
                        }
                        //if a PK lookup did not match at all
                        if ( curr == null ) {
                            if ( operation != Operation.SELECT ) {
                                current = Long.valueOf( 0L );
                                return true;
                            }
                            return false;
                        }
                        currentFile = lookupFile;
                        current = Long.valueOf( 1L );
                    } else {
                        curr = fileToRow( currentFile );
                        //todo
                        if ( curr == null ) {
                            if ( operation != Operation.SELECT ) {
                                current = Long.valueOf( updateDeleteCount );
                                return true;
                            }
                            return false;
                        }
                        if ( !condition.matches( (Object[]) curr, columnTypes, dataContext ) ) {
                            fileListPosition++;
                            continue;
                        }
                    }
                } else {
                    curr = fileToRow( currentFile );
                    if ( curr == null ) {
                        fileListPosition++;
                        continue;
                    }
                }

                if ( operation == Operation.SELECT ) {
                    //project only if necessary (if a projection and condition is given)
                    if ( projectionMapping != null && condition != null ) {
                        curr = project( curr );
                    }
                    Object[] o = (Object[]) curr;
                    if ( o.length == 1 ) {
                        current = o[0];
                    } else {
                        // If all values are null: continue
                        //this can happen, if we iterate over multiple nullable columns, because the fileList comes from a PK-column that is NOT NULL
                        if ( curr == null ) {
                            fileListPosition++;
                            continue;
                        }
                        current = curr;
                    }
                    fileListPosition++;
                    return true;
                } else if ( operation == Operation.DELETE ) {
                    for ( File colFolder : columnFolders ) {
                        File source = new File( colFolder, currentFile.getName() );
                        File target = new File( colFolder, getNewFileName( Operation.DELETE, currentFile.getName() ) );
                        if ( source.exists() ) {
                            Files.move( source.toPath(), target.toPath() );
                        }
                    }
                    updateDeleteCount++;
                    current = Long.valueOf( updateDeleteCount );
                    fileListPosition++;
                    //continue;
                } else if ( operation == Operation.UPDATE ) {
                    Object[] updateObj = new Object[columnFolders.size()];
                    Set<Integer> updatedColumns = new HashSet<>();
                    for ( int c = 0; c < columnFolders.size(); c++ ) {
                        if ( updates.containsKey( c ) ) {
                            updateObj[c] = updates.get( c ).getValue( dataContext, 0 );
                            updatedColumns.add( c );
                        } else {
                            //needed for the hash
                            updateObj[c] = ((Object[]) curr)[c];
                        }
                    }
                    int newHash = hashRow( updateObj );
                    String oldFileName = FileStore.SHA.hashString( String.valueOf( hashRow( curr ) ), FileStore.CHARSET ).toString();

                    int j = 0;
                    for ( File colFolder : columnFolders ) {
                        File source = new File( colFolder, oldFileName );

                        // write new file
                        if ( updateObj[j] != null ) {
                            File insertFile = new File( colFolder, getNewFileName( Operation.INSERT, String.valueOf( newHash ) ) );
                            /*if( newFile.exists() ) {
                                throw new RuntimeException("This update would lead to a primary key conflict, " + newFile.getAbsolutePath());
                            }*/

                            //if column has not been updated: just copy the old file to the new one with the PK in the new fileName
                            if ( !updatedColumns.contains( j ) && source.exists() ) {
                                Files.copy( source.toPath(), insertFile.toPath() );
                            } else {
                                //Write updated value. Overrides file if it exists (if you have a double update on the same item)
                                if ( updateObj[j] instanceof InputStream ) {
                                    FileUtils.copyInputStreamToFile( ((InputStream) updateObj[j]), insertFile );
                                } else {
                                    Files.write( insertFile.toPath(), updateObj[j].toString().getBytes( FileStore.CHARSET ) );
                                }
                            }
                        }

                        File deleteFile = new File( colFolder, getNewFileName( Operation.DELETE, String.valueOf( hashRow( curr ) ) ) );
                        if ( source.exists() ) {
                            Files.move( source.toPath(), deleteFile.toPath() );
                        }
                        j++;
                    }

                    updateDeleteCount++;
                    current = Long.valueOf( updateDeleteCount );
                    fileListPosition++;
                    //continue;
                } else {
                    throw new RuntimeException( operation + " operation is not supported in FileEnumerator" );
                }
            }
        } catch ( IOException | RuntimeException e ) {
            throw new RuntimeException( e );
        }
    }


    /**
     * Convert files to a row
     *
     * @param currentFile The filename of the {@code currentFile} is used to find the files in the respective column folders
     * @return Null if the file does not exists (in case of a PK lookup) or the row as an array of objects.
     */
    @Nullable
    private Object fileToRow( final File currentFile ) throws IOException {
        Object[] curr = new Object[numOfCols];
        int i = 0;
        boolean allNull = true;
        for ( File colFolder : columnFolders ) {
            File f = new File( colFolder, currentFile.getName() );
            String s = null;
            byte[] encoded = null;
            Byte[] encoded2 = null;
            if ( f.exists() ) {
                if ( columnTypes[i].getFamily() == PolyTypeFamily.MULTIMEDIA ) {
                    if ( dataContext.getStatement().getTransaction().getFlavor() == MultimediaFlavor.DEFAULT ) {
                        encoded = Files.readAllBytes( f.toPath() );
                    }
                } else {
                    s = new String( Files.readAllBytes( f.toPath() ), FileStore.CHARSET );
                    if ( s.equals( "" ) ) {
                        curr[i] = null;
                        i++;
                        continue;
                    }
                }
            } else {
                curr[i] = null;
                i++;
                continue;
            }
            allNull = false;
            if ( columnTypes[i].getFamily() == PolyTypeFamily.MULTIMEDIA ) {
                if ( dataContext.getStatement().getTransaction().getFlavor() == MultimediaFlavor.DEFAULT ) {
                    curr[i] = encoded;
                } else {
                    curr[i] = f;
                }
            } else {
                curr[i] = PolyTypeUtil.stringToObject( s, columnTypes[i] );
            }
            i++;
        }
        return allNull ? null : curr;
    }


    private Object project( final Object o1 ) {
        Object[] o = (Object[]) o1;
        assert (projectionMapping != null);
        Object[] out = new Object[projectionMapping.length];
        for ( int i = 0; i < projectionMapping.length; i++ ) {
            out[i] = o[projectionMapping[i]];
        }
        return out;
    }


    @Override
    public void reset() {
        fileListPosition = 0;
    }


    @Override
    public void close() {

    }


    /**
     * Hash only the elements of a row that are part of the primary key
     */
    int hashRow( final Object row ) {
        Object[] toHash = new Object[pkMapping.length];
        for ( int i = 0; i < pkMapping.length; i++ ) {
            toHash[i] = ((Object[]) row)[pkMapping[i]].toString();
        }
        return Arrays.hashCode( toHash );
    }


    @SuppressWarnings("UnstableApiUsage")
    String getNewFileName( final Operation operation, final String hashCode ) {
        String operationAbbreviation;//must be of length 3!
        switch ( operation ) {
            case INSERT:
                operationAbbreviation = "ins";
                break;
            case DELETE:
                operationAbbreviation = "del";
                break;
            default:
                throw new RuntimeException( "Did not expect operation " + operation );
        }
        return "_"// underline at the beginning of files that are not yet committed
                + operationAbbreviation
                + "_"
                //XID
                + FileStore.SHA.hashString( dataContext.getStatement().getTransaction().getXid().toString(), FileStore.CHARSET ).toString()
                + "_"
                //PK hash
                + FileStore.SHA.hashString( hashCode, FileStore.CHARSET ).toString();
    }

}
