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
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;


@Slf4j
public class FileEnumerator<E> implements Enumerator<E> {

    private E current;
    private final HashMap<Long, File> columnFolders = new HashMap<>();
    private Integer maxRowCount;
    private Long colWithMostRows;
    private final File[] fileList;
    private int fileListPosition = 0;
    private final int numOfCols;
    private final AtomicBoolean cancelFlag;
    private final List<PolyType> columnTypes;
    private final Gson gson;
    private final FileStore store;
    private final Charset encoding = StandardCharsets.UTF_8;
    private final Object[] filterValues;

    FileEnumerator( final FileStore store, final List<Long> columnIds, final List<PolyType> columnTypes, final AtomicBoolean cancelFlag, Object[] filterValues ) {
        this.cancelFlag = cancelFlag;
        this.columnTypes = columnTypes;
        this.gson = new Gson();
        this.store = store;
        for( Long colId : columnIds ) {
            File columnFolder = store.getColumnFolder( colId );
            columnFolders.put( colId, columnFolder );
            int currentColumnSize = columnFolder.listFiles() == null ? 0 : columnFolder.listFiles().length;
            if( maxRowCount == null || maxRowCount < currentColumnSize ) {
                maxRowCount = currentColumnSize;
                colWithMostRows = colId;
            }
        }
        this.fileList = columnFolders.get( colWithMostRows ).listFiles();
        numOfCols = columnFolders.size();
        this.filterValues = filterValues;
    }

    FileEnumerator( final FileStore store, final List<Long> columnIds, final List<PolyType> columnTypes, final AtomicBoolean cancelFlag ) {
        this( store, columnIds, columnTypes, cancelFlag, new Object[0] );
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
            for( ; ; ) {
                if( cancelFlag.get() ) {
                    return false;
                }
                if( fileListPosition >= fileList.length ) {
                    return false;
                }
                File currentFile = fileList[fileListPosition];
                String[] strings = new String[numOfCols];
                Object[] curr = new Object[numOfCols];
                int i = 0;
                for( File colFolder : this.columnFolders.values() ) {
                    File f = new File(colFolder, currentFile.getName() );
                    String s;
                    if( !f.exists() ) {
                        s = null;
                    } else {
                        byte[] encoded = Files.readAllBytes( f.toPath() );
                        s = new String( encoded, encoding );
                    }
                    if( filterValues.length == numOfCols ) {
                        Object filter = filterValues[i];
                        if( filter != null ){
                            if( s != null && !s.equals( filter.toString() )) {
                                fileListPosition++;
                                continue outer;
                            }
                        }
                    }
                    strings[i] = s;
                    if( s == null ) {
                        curr[i] = null;
                    } else {
                        switch ( columnTypes.get( i ) ) {
                            //todo add support for more types
                            case BOOLEAN:
                                curr[i] = gson.fromJson( s, Boolean.class );
                                break;
                            case INTEGER:
                                curr[i] = Integer.parseInt(s);
                                break;
                            case DOUBLE:
                                curr[i] = Double.parseDouble( s );
                            case FLOAT:
                                curr[i] = Float.parseFloat( s );
                            default:
                                curr[i] = s;
                        }
                    }
                    i++;
                }
                current = (E) curr;
                fileListPosition++;
                return true;
            }
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }
    }

    @Override
    public void reset() {
        fileListPosition = 0;
    }

    @Override
    public void close() {

    }
}
