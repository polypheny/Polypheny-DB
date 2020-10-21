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


import com.google.gson.Gson;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.file.rel.FileFilter;
import org.polypheny.db.adapter.file.rel.FileFilter.Condition;
import org.polypheny.db.type.PolyType;


@Slf4j
public class FileEnumerator<E> implements Enumerator<E> {

    E current;
    final List<File> columnFolders = new ArrayList<>();
    Integer maxRowCount;
    Long colWithMostRows;
    final File[] fileList;
    int fileListPosition = 0;
    final int numOfCols;
    final DataContext dataContext;
    final Condition condition;
    final Integer[] projectionMapping;
    final PolyType[] columnTypes;
    final Gson gson;
    final Charset encoding = StandardCharsets.UTF_8;

    /**
     * FileEnumerator
     * When no filter is available, it will only iterate the files in the projected columns
     * If a filter is available, it will iterate over all columns and project each row
     *
     * @param rootPath The rootPath is required to know where the files to iterate are placed
     * @param columnIds Ids of the columns that come from a tableScan. If there is no filter, the enumerator will only iterate over the columns that are specified by the projection
     * @param columnTypes DataTypes of the columns that are given by the {@code columnIds} array
     * @param projectionMapping Mapping on how to project a table. E.g. the array [3,2] means that the row [a,b,c,d,e] will be projected to [c,b]
     * @param dataContext DataContext
     * @param condition Condition that can be {@code null}. The columnReferences in the filter point to the columns coming from the tableScan, not from the projection
     */
    public FileEnumerator( final String rootPath,
            final Long[] columnIds,
            final PolyType[] columnTypes,
            final Integer[] projectionMapping,
            final DataContext dataContext,
            final FileFilter.Condition condition ) {
        this.dataContext = dataContext;
        this.condition = condition;
        this.projectionMapping = projectionMapping;
        this.gson = new Gson();
        Long[] columnsToIterate = columnIds;
        // If there is a projection and no filter, it is sufficient to just load the data of the projected columns.
        // If a filter is given, the whole table has to be loaded.
        if ( condition == null && projectionMapping != null ) {
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
        for ( Long colId : columnsToIterate ) {
            File columnFolder = FileStore.getColumnFolder( rootPath, colId );
            columnFolders.add( columnFolder );
            int currentColumnSize = columnFolder.listFiles() == null ? 0 : columnFolder.listFiles().length;
            if ( maxRowCount == null || maxRowCount < currentColumnSize ) {
                maxRowCount = currentColumnSize;
                colWithMostRows = colId;
            }
        }
        this.fileList = FileStore.getColumnFolder( rootPath, colWithMostRows ).listFiles();
        numOfCols = columnFolders.size();
    }

    @Override
    public E current() {
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
                    return false;
                }
                File currentFile = fileList[fileListPosition];
                String[] strings = new String[numOfCols];
                Comparable[] curr = new Comparable[numOfCols];
                int i = 0;
                //todo fix wrong order
                for ( File colFolder : columnFolders ) {
                    File f = new File( colFolder, currentFile.getName() );
                    String s;
                    if ( !f.exists() ) {
                        s = null;
                    } else {
                        byte[] encoded = Files.readAllBytes( f.toPath() );
                        s = new String( encoded, encoding );
                    }
                    strings[i] = s;
                    if ( s == null || s.equals( "" ) ) {
                        curr[i] = null;
                    } else {
                        switch ( columnTypes[i] ) {
                            //todo add support for more types
                            case BOOLEAN:
                                curr[i] = gson.fromJson( s, Boolean.class );
                                break;
                            case INTEGER:
                            case TIME:
                            case DATE:
                                curr[i] = Integer.parseInt( s );
                                break;
                            case BIGINT:
                            case TIMESTAMP:
                                curr[i] = Long.parseLong( s );
                                break;
                            case DOUBLE:
                                curr[i] = Double.parseDouble( s );
                                break;
                            case FLOAT:
                                curr[i] = Float.parseFloat( s );
                                break;
                            //case ARRAY:
                            default:
                                curr[i] = s;
                        }
                    }
                    i++;
                }
                if ( condition != null ) {
                    if ( !condition.matches( curr, dataContext ) ) {
                        fileListPosition++;
                        continue;
                    }
                }
                //project only if necessary (if a projection and condition is given)
                if ( projectionMapping != null && condition != null ) {
                    curr = project( curr );
                }
                if ( curr.length == 1 ) {
                    current = (E) curr[0];
                } else {
                    current = (E) curr;
                }
                fileListPosition++;
                return true;
            }
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }
    }

    private Comparable[] project( final Comparable[] o ) {
        assert (projectionMapping != null);
        Comparable[] out = new Comparable[projectionMapping.length];
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
}
