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
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.commons.lang.math.NumberUtils;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.file.FileRel.FileImplementor.Operation;
import org.polypheny.db.type.PolyType;


@Slf4j
public class FileEnumerator<E> implements Enumerator<E> {

    E current;
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
    final Map<Integer, Update> updates = new HashMap<>();
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
            final Update[] updates ) {

        this.operation = operation;
        if ( operation == Operation.DELETE || operation == Operation.UPDATE ) {
            //fix to make sure current is never null
            current = (E) Long.valueOf( 0L );
        }
        this.dataContext = dataContext;
        this.condition = condition;
        this.projectionMapping = projectionMapping;

        if ( updates != null ) {
            //in case of an UPDATE, the projectionMapping represent the indexes of the columns that will be updated
            for ( Update update : updates ) {
                this.updates.put( update.getColumnReference(), update );
            }
        }

        //pkMapping
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
        //We want to read data where an insert has been prepared and skip data where a deletion has been prepared.
        @SuppressWarnings("UnstableApiUsage")
        String xidHash = FileStore.SHA.hashString( dataContext.getStatement().getTransaction().getXid().toString(), FileStore.CHARSET ).toString();
        FileFilter fileFilter = file -> !file.isHidden() && (!file.getName().startsWith( "_" ) || file.getName().startsWith( "_ins_" + xidHash ));
        for ( Long colId : columnsToIterate ) {
            File columnFolder = FileStore.getColumnFolder( rootPath, colId );
            columnFolders.add( columnFolder );
        }
        if ( columnsToIterate.length == 1 ) {
            //if we go over a single column, we can iterate it, even if null values are not present as files
            this.fileList = FileStore.getColumnFolder( rootPath, columnsToIterate[0] ).listFiles( fileFilter );
        } else {
            //iterate over a PK-column, because they are always NOT NULL
            this.fileList = FileStore.getColumnFolder( rootPath, pkIds.get( 0 ) ).listFiles( fileFilter );
        }
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
                File currentFile = fileList[fileListPosition];
                String[] strings = new String[numOfCols];
                Comparable[] curr = new Comparable[numOfCols];
                int i = 0;
                for ( File colFolder : columnFolders ) {
                    File f = new File( colFolder, currentFile.getName() );
                    String s;
                    if ( !f.exists() ) {
                        s = null;
                    } else {
                        byte[] encoded = Files.readAllBytes( f.toPath() );
                        s = new String( encoded, FileStore.CHARSET );
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
                                curr[i] = Integer.parseInt( s );
                                break;
                            case TIME:
                                if ( NumberUtils.isNumber( s ) ) {
                                    curr[i] = Integer.parseInt( s );
                                } else {
                                    //final DateTimeUtils.PrecisionTime ts = DateTimeUtils.parsePrecisionDateTimeLiteral( s, new SimpleDateFormat( DateTimeUtils.TIME_FORMAT_STRING, Locale.ROOT ), DateTimeUtils.UTC_ZONE, -1 );
                                    /*final DateTimeUtils.PrecisionTime ts = DateTimeUtils.parsePrecisionDateTimeLiteral( s, new SimpleDateFormat( DateTimeUtils.TIMESTAMP_FORMAT_STRING, Locale.ROOT ), DateTimeUtils.UTC_ZONE, -1 );
                                    if( ts == null ) {
                                        log.warn( "could not parse TIME " + s );
                                    }
                                    curr[i] = TimeString.fromCalendarFields( ts.getCalendar() ).toString();
                                    */
                                    if ( operation == Operation.SELECT && s.length() == 19 ) {
                                        curr[i] = s.substring( 11 );
                                    } else {
                                        curr[i] = s;
                                    }
                                }
                                break;
                            case DATE:
                                if ( NumberUtils.isNumber( s ) ) {
                                    curr[i] = Integer.parseInt( s );
                                } else {
                                    //final Calendar cal = DateTimeUtils.parseDateFormat( s, new SimpleDateFormat( DateTimeUtils.DATE_FORMAT_STRING, Locale.ROOT ), DateTimeUtils.UTC_ZONE );
                                    /*final DateTimeUtils.PrecisionTime ts = DateTimeUtils.parsePrecisionDateTimeLiteral( s, new SimpleDateFormat( DateTimeUtils.TIMESTAMP_FORMAT_STRING, Locale.ROOT ), DateTimeUtils.UTC_ZONE, -1 );
                                    if( ts == null ) {
                                        log.warn( "could not parse DATE " + s );
                                    }
                                    curr[i] = DateString.fromCalendarFields( ts.getCalendar() ).toString();
                                    */
                                    if ( operation == Operation.SELECT && s.length() == 19 ) {
                                        curr[i] = s.substring( 0, 10 );
                                    } else {
                                        curr[i] = s;
                                    }
                                }
                                break;
                            case TIMESTAMP:
                                if ( NumberUtils.isNumber( s ) ) {
                                    curr[i] = Long.parseLong( s );
                                } else {
                                    /*final DateTimeUtils.PrecisionTime ts = DateTimeUtils.parsePrecisionDateTimeLiteral( s, new SimpleDateFormat( DateTimeUtils.TIMESTAMP_FORMAT_STRING, Locale.ROOT ), DateTimeUtils.UTC_ZONE, -1 );
                                    if( ts == null ) {
                                        log.warn( "could not parse TIMESTAMP " + s );
                                    }
                                    curr[i] = TimestampString.fromCalendarFields( ts.getCalendar() ).toString();
                                    */
                                    curr[i] = s;
                                }
                                break;
                            case BIGINT:
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
                if ( operation == Operation.SELECT ) {
                    //project only if necessary (if a projection and condition is given)
                    if ( projectionMapping != null && condition != null ) {
                        curr = project( curr );
                    }
                    if ( curr.length == 1 ) {
                        current = (E) curr[0];
                    } else {
                        //if all values are null: continue
                        //this can happen, if we iterate over multiple nullable columns, because the fileList comes from a PK-column that is NOT NULL
                        if ( Arrays.stream( curr ).allMatch( Objects::isNull ) ) {
                            fileListPosition++;
                            continue;
                        }
                        current = (E) curr;
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
                    current = (E) updateDeleteCount;
                    fileListPosition++;
                    //continue;
                } else if ( operation == Operation.UPDATE ) {
                    Object[] updateObj = new Object[columnFolders.size()];
                    for ( int c = 0; c < columnFolders.size(); c++ ) {
                        if ( updates.containsKey( c ) ) {
                            updateObj[c] = updates.get( c ).getValue( dataContext );
                        } else {
                            updateObj[c] = curr[c];
                        }
                        //convert date/time/timestamp (that come as int/long) to string
                        /*if( updateObj[c] == null ) {
                            continue;
                        }
                        switch ( columnTypes[c] ) {
                            case DATE:
                                //updateObj[c] = DateTimeUtils.unixDateToString( (Integer) updateObj[c] ) + " " + LocalTime.MIN.format( DateTimeFormatter.ofPattern( DateTimeUtils.TIME_FORMAT_STRING ) );
                                updateObj[c] = DateTimeUtils.unixDateToString( Integer.parseInt( (String) updateObj[c] ) ) + " " + LocalTime.MIN.format( DateTimeFormatter.ofPattern( DateTimeUtils.TIME_FORMAT_STRING ) );
                                break;
                            case TIME:
                                //updateObj[c] = DateTimeUtils.unixTimestampToString( (Integer) updateObj[c] );
                                updateObj[c] = DateTimeUtils.unixTimestampToString( Integer.parseInt( (String) updateObj[c] ) );
                                break;
                            case TIMESTAMP:
                                //updateObj[c] = DateTimeUtils.unixTimestampToString( (Long) updateObj[c] );
                                updateObj[c] = DateTimeUtils.unixTimestampToString( Long.parseLong( (String) updateObj[c] ) );
                                break;
                        }*/
                    }
                    int newHash = hashRow( updateObj );
                    String oldFileName = FileStore.SHA.hashString( String.valueOf( hashRow( curr ) ), FileStore.CHARSET ).toString();

                    int j = 0;
                    for ( File colFolder : columnFolders ) {
                        //File source = new File( colFolder, currentFile.getName() );
                        File source = new File( colFolder, oldFileName );
                        //File target = new File( colFolder, getNewFileName( Operation.DELETE, currentFile.getName() ) );
                        File target = new File( colFolder, getNewFileName( Operation.DELETE, String.valueOf( hashRow( curr ) ) ) );
                        if ( source.exists() ) {
                            Files.move( source.toPath(), target.toPath() );
                        }

                        //write new file
                        if ( updateObj[j] != null ) {
                            File newFile = new File( colFolder, getNewFileName( Operation.INSERT, String.valueOf( newHash ) ) );
                            /*if( newFile.exists() ) {
                                throw new RuntimeException("This update would lead to a primary key conflict, " + newFile.getAbsolutePath());
                            }*/
                            Files.write( newFile.toPath(), updateObj[j].toString().getBytes( FileStore.CHARSET ) );
                        }
                        j++;
                    }

                    updateDeleteCount++;
                    current = (E) updateDeleteCount;
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

    /**
     * Hash only the elements of a row that are part of the primary key
     */
    int hashRow( final Object[] row ) {
        Object[] toHash = new Object[pkMapping.length];
        for ( int i = 0; i < pkMapping.length; i++ ) {
            toHash[i] = row[pkMapping[i]].toString();
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
