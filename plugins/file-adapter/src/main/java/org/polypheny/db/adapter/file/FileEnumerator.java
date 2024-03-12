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
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.commons.io.FileUtils;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.file.FileAlg.FileImplementor.Operation;
import org.polypheny.db.adapter.file.Value.InputValue;
import org.polypheny.db.adapter.file.Value.ValueType;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction.MultimediaFlavor;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.type.entity.PolyLong;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyBlob;


@Slf4j
public class FileEnumerator implements Enumerator<PolyValue[]> {

    private final Runnable updateFiles;
    PolyValue[] current;
    final Operation operation;
    final List<File> columnFolders = new ArrayList<>();
    File[] fileList;
    Integer fileListPosition = 0;
    Long updateDeleteCount = 0L;
    boolean updatedOrDeleted = false;
    final int numOfCols;
    final DataContext dataContext;
    final Condition condition;
    final List<Value> projectionMapping;
    List<AlgDataTypeField> columnTypes;

    List<AlgDataTypeField> projectedTypes;
    final Map<Integer, Value> updates = new HashMap<>();
    final Integer[] pkMapping;
    final File hardlinkFolder;
    private boolean ongoing = true;


    /**
     * FileEnumerator
     * When no filter is available, it will only iterate the files in the projected columns
     * If a filter is available, it will iterate over all columns and project each row
     *
     * @param rootPath The rootPath is required to know where the files to iterate are placed
     * @param partitionId The id of the partition
     * @param columnIds Ids of the columns that come from a tableScan. If there is no filter, the enumerator will only iterate over the columns that are specified by the projection
     * @param entity The entity that is target of the operation
     * @param projectionMapping Mapping on how to project a table. E.g. the array [3,2] means that the row [a,b,c,d,e] will be projected to [c,b].
     * In case of an UPDATE operation, the projectionMapping represents the indexes of the columns that will be updated, e.g. [2,3] means that b and c will be updated.
     * @param dataContext DataContext
     * @param condition Condition that can be {@code null}. The columnReferences in the filter point to the columns coming from the tableScan, not from the projection
     */
    public FileEnumerator(
            final Operation operation,
            final String rootPath,
            final Long partitionId,
            final Long[] columnIds,
            final FileTranslatableEntity entity,
            final List<Long> pkIds,
            final @Nullable List<Value> projectionMapping,
            final DataContext dataContext,
            final @Nullable Condition condition,
            final @Nullable List<List<PolyValue>> updates ) {

        this.operation = operation;
        if ( operation == Operation.DELETE || operation == Operation.UPDATE ) {
            //fix to make sure current is never null
            current = new PolyLong[]{ PolyLong.of( 0L ) };
        }
        this.dataContext = operation == Operation.INSERT ? dataContext : new EnumerableDataContext( dataContext );

        this.condition = condition;
        this.projectionMapping = projectionMapping;

        if ( updates != null ) {
            // In case of an UPDATE, the projectionMapping represent the indexes of the columns that will be updated
            for ( PolyValue update : updates.get( 0 ) ) {
                this.updates.put( ((Value) update).getColumnReference(), (Value) update );
            }
        }

        // pkMapping
        Integer[] pkMapping = new Integer[pkIds.size()];
        int ii = 0;
        List<Long> colIdsAsList = List.of( columnIds );
        for ( long pkId : pkIds ) {
            pkMapping[ii] = colIdsAsList.indexOf( pkId );
            ii++;
        }
        this.pkMapping = pkMapping;

        // If there is a projection and no filter, it is sufficient to just load the data of the projected columns.
        // If a filter is given, the whole table has to be loaded (because of the column references)
        // If we have an UPDATE operation, the whole table has to be loaded as well, to generate the hashes
        this.columnTypes = entity.getTupleType().getFields();
        this.projectedTypes = entity.getTupleType().getFields();

        if ( condition == null && projectionMapping != null && operation != Operation.UPDATE ) {
            List<AlgDataTypeField> projectedTypes = new ArrayList<>( Collections.nCopies( projectionMapping.size(), null ) );
            for ( int i = 0; i < projectionMapping.size(); i++ ) {
                Value value = projectionMapping.get( i );
                AlgDataTypeField field = null;
                if ( value.valueType == ValueType.INPUT ) {
                    int index = ((InputValue) projectionMapping.get( i )).getIndex();
                    field = entity.getTupleType().getFields().get( index );
                }
                projectedTypes.set( i, field );
            }
            this.projectedTypes = projectedTypes;
        }
        // We want to read data where an insert has been prepared and skip data where a deletion has been prepared.
        String xidHash = FileStore.SHA.hashString( dataContext.getStatement().getTransaction().getXid().toString(), FileStore.CHARSET ).toString();
        FileFilter fileFilter = file -> !file.isHidden() && !file.getName().startsWith( "~$" ) && (!file.getName().startsWith( "_" ) || file.getName().startsWith( "_ins_" + xidHash ));
        for ( long colId : columnIds ) {
            File columnFolder = FileStore.getColumnFolder( rootPath, colId, partitionId );
            columnFolders.add( columnFolder );
        }

        // If we go over a single column, we can iterate it, even if null values are not present as files
        this.updateFiles = () -> {
            this.fileList = FileStore.getColumnFolder( rootPath, columnIds[0], partitionId ).listFiles( fileFilter );
        };
        this.updateFiles.run();

        numOfCols = columnFolders.size();
        // create folder for the hardlinks
        this.hardlinkFolder = new File( rootPath, "hardlinks/" + xidHash );
        if ( !hardlinkFolder.exists() ) {
            if ( !hardlinkFolder.mkdirs() ) {
                throw new GenericRuntimeException( "Could not create hardlink directory " + hardlinkFolder.getAbsolutePath() );
            }
        }
    }


    public void toDefault() {
        fileListPosition = 0;
        updateDeleteCount = 0L;
        updatedOrDeleted = false;
        // we have to update the files, because the files might have changed
        updateFiles.run();
    }


    @Override
    public PolyValue[] current() {
        return current;
    }


    @Override
    public boolean moveNext() {
        if ( operation == Operation.SELECT ) {
            return singleNext();
        } else if ( operation == Operation.INSERT ) {
            throw new GenericRuntimeException( "Not supported" );
        }
        if ( !ongoing ) {
            return false;
        }

        ongoing = singleNext();
        if ( ((EnumerableDataContext) dataContext).isEmpty() ) {
            ongoing = false;
            return true;
        }
        toDefault();
        ((EnumerableDataContext) dataContext).next();
        return true;
    }


    private boolean singleNext() {
        //todo make sure that all requirements of the interface are satisfied
        try {
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
                List<PolyValue> curr;

                if ( condition != null ) {
                    curr = fileToRow( currentFile );

                    if ( curr == null ) {
                        if ( operation != Operation.SELECT ) {
                            current = new PolyLong[]{ PolyLong.of( updateDeleteCount ) };
                            return true;
                        }
                        return false;
                    }

                    if ( !condition.matches( curr, columnTypes, dataContext ) ) {
                        fileListPosition++;
                        continue;
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
                    if ( projectionMapping != null ) {//|| condition != null ) {
                        curr = project( curr, dataContext );
                    }

                    // If all values are null: continue
                    //this can happen, if we iterate over multiple nullable columns, because the fileList comes from a PK-column that is NOT NULL
                    current = curr.toArray( new PolyValue[0] );

                    fileListPosition++;
                    return true;
                } else if ( operation == Operation.DELETE ) {
                    handleDelete( currentFile );
                } else if ( operation == Operation.UPDATE ) {
                    handleUpdate( curr );
                } else {
                    throw new GenericRuntimeException( operation + " operation is not supported in FileEnumerator" );
                }
            }
        } catch ( IOException | RuntimeException e ) {
            throw new GenericRuntimeException( e );
        }
    }


    private void handleDelete( File currentFile ) throws IOException {
        for ( File colFolder : columnFolders ) {
            File source = new File( colFolder, currentFile.getName() );
            File target = new File( colFolder, getNewFileName( Operation.DELETE, currentFile.getName() ) );
            if ( source.exists() ) {
                Files.move( source.toPath(), target.toPath() );
            }
        }
        updateDeleteCount++;
        current = new PolyLong[]{ PolyLong.of( updateDeleteCount ) };
        fileListPosition++;
        //continue;
    }


    private void handleUpdate( List<PolyValue> curr ) throws IOException {
        List<PolyValue> updateObj = new ArrayList<>( Collections.nCopies( columnFolders.size(), null ) );
        Set<Integer> updatedColumns = new HashSet<>();
        for ( int c = 0; c < columnFolders.size(); c++ ) {
            if ( updates.containsKey( c ) ) {
                updateObj.set( c, updates.get( c ).getValue( curr, dataContext, 0 ) );
                updatedColumns.add( c );
            } else {
                //needed for the hash
                updateObj.set( c, curr.get( c ) );
            }
        }
        int newHash = hashRow( updateObj );
        String oldFileName = FileStore.SHA.hashString( String.valueOf( hashRow( curr ) ), FileStore.CHARSET ).toString();

        int j = 0;
        for ( File colFolder : columnFolders ) {
            File source = new File( colFolder, oldFileName );

            // write new file

            File insertFile = new File( colFolder, getNewFileName( Operation.INSERT, String.valueOf( newHash ) ) );

            //if column has not been updated: just copy the old file to the new one with the PK in the new fileName
            if ( !updatedColumns.contains( j ) && source.exists() ) {
                Files.copy( source.toPath(), insertFile.toPath() );
            } else {
                // Write updated value. Overrides file if it exists (if you have a double update on the same item)
                if ( updateObj.get( j ) == null || updateObj.get( j ).isNull() ) {
                    Files.writeString( insertFile.toPath(), PolyNull.NULL.toTypedJson(), FileStore.CHARSET );
                } else if ( updateObj.get( j ).isBlob() && updateObj.get( j ).asBlob().isHandle() ) {
                    if ( insertFile.exists() && !insertFile.delete() ) {
                        throw new GenericRuntimeException( "Could not delete temporary insert file" );
                    }
                    updateObj.get( j ).asBlob().getHandle().materializeAsFile( insertFile.toPath() );
                } else if ( updateObj.get( j ).isBlob() ) {
                    FileUtils.copyInputStreamToFile( updateObj.get( j ).asBlob().stream, insertFile );
                } else {
                    Files.writeString( insertFile.toPath(), updateObj.get( j ).toTypedJson(), FileStore.CHARSET );
                }
            }

            File deleteFile = new File( colFolder, getNewFileName( Operation.DELETE, String.valueOf( hashRow( curr ) ) ) );
            if ( source.exists() ) {
                Files.move( source.toPath(), deleteFile.toPath() );
            }
            j++;
        }

        updateDeleteCount++;
        current = new PolyLong[]{ PolyLong.of( updateDeleteCount ) };
        fileListPosition++;
        //continue;
    }


    /**
     * Convert files to a row
     *
     * @param currentFile The filename of the {@code currentFile} is used to find the files in the respective column folders
     * @return Null if the file does not exists (in case of a PK lookup) or the row as an array of objects.
     */
    @Nullable
    private List<PolyValue> fileToRow( final File currentFile ) throws IOException {
        List<PolyValue> curr = new ArrayList<>( Collections.nCopies( numOfCols, null ) );
        int i = 0;
        boolean allNull = true;
        for ( File colFolder : columnFolders ) {
            File f = new File( colFolder, currentFile.getName() );
            String s;

            if ( f.exists() ) {
                s = Files.readString( f.toPath(), FileStore.CHARSET );
                if ( s.isEmpty() ) {
                    curr.set( i, null );
                    i++;
                    continue;
                }
            } else {
                curr.set( i, null );
                i++;
                continue;
            }
            allNull = false;
            if ( columnTypes.get( i ).getType().getPolyType().getFamily() == PolyTypeFamily.MULTIMEDIA ) {
                if ( dataContext.getStatement().getTransaction().getFlavor() == MultimediaFlavor.DEFAULT ) {
                    curr.set( i, PolyTypeUtil.stringToObject( s, columnTypes.get( i ) ) );
                } else {
                    File hardLink = new File( hardlinkFolder, colFolder.getName() + "_" + f.getName() );
                    if ( !hardLink.exists() ) {
                        Files.createLink( hardLink.toPath(), f.toPath() );
                    }
                    //curr[i] = f;
                    curr.set( i, PolyBlob.of( hardLink ) );
                }
            } else {
                curr.set( i, PolyTypeUtil.stringToObject( s, columnTypes.get( i ) ) );
            }
            i++;
        }
        return allNull ? null : curr;
    }


    private List<PolyValue> project( final List<PolyValue> o1, DataContext dataContext ) {
        assert (projectionMapping != null);
        List<PolyValue> out = new ArrayList<>( Collections.nCopies( projectionMapping.size(), null ) );
        for ( int i = 0; i < projectionMapping.size(); i++ ) {
            out.set( i, projectionMapping.get( i ).getValue( o1, dataContext, 0 ) );
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
    int hashRow( final List<PolyValue> row ) {
        Object[] toHash = new Object[pkMapping.length];
        for ( int i = 0; i < pkMapping.length; i++ ) {
            PolyValue obj = row.get( pkMapping[i] );
            toHash[i] = obj != null ? obj.toJson() : "";
        }
        return Arrays.hashCode( toHash );
    }


    String getNewFileName( final Operation operation, final String hashCode ) {
        String operationAbbreviation = switch ( operation ) {
            case INSERT -> "ins";
            case DELETE -> "del";
            default -> throw new GenericRuntimeException( "Did not expect operation " + operation );
        };//must be of length 3!
        return "_"// underline at the beginning of files that are not yet committed
                + operationAbbreviation
                + "_"
                //XID
                + FileStore.SHA.hashString( dataContext.getStatement().getTransaction().getXid().toString(), FileStore.CHARSET )
                + "_"
                //PK hash
                + FileStore.SHA.hashString( hashCode, FileStore.CHARSET );
    }


    public static class EnumerableDataContext implements DataContext {

        private final DataContext dataContext;
        @Getter
        private final Snapshot snapshot;
        @Getter
        private final JavaTypeFactory typeFactory;
        @Getter
        private final QueryProvider queryProvider;
        @Getter
        private final Statement statement;
        @Getter
        private final int batches;

        AtomicLong counter = new AtomicLong();


        public EnumerableDataContext( DataContext dataContext ) {
            this.dataContext = dataContext;
            this.snapshot = dataContext.getSnapshot();
            this.typeFactory = dataContext.getTypeFactory();
            this.queryProvider = dataContext.getQueryProvider();
            this.statement = dataContext.getStatement();
            this.batches = dataContext.getParameterValues().size();
        }


        public void next() {
            counter.incrementAndGet();
        }


        public boolean isEmpty() {
            return counter.get() >= batches - 1;
        }


        @Override
        public Object get( String name ) {
            return dataContext.get( name );
        }


        @Override
        public void addAll( Map<String, Object> map ) {
            throw new GenericRuntimeException( "Not supported" );
        }


        @Override
        public void addParameterValues( long index, AlgDataType type, List<PolyValue> data ) {
            throw new GenericRuntimeException( "Not supported" );
        }


        @Override
        public AlgDataType getParameterType( long index ) {
            return dataContext.getParameterType( index + counter.get() );
        }


        @Override
        public List<Map<Long, PolyValue>> getParameterValues() {
            return List.of( dataContext.getParameterValues().get( counter.intValue() ) );
        }


        @Override
        public void setParameterValues( List<Map<Long, PolyValue>> values ) {
            throw new GenericRuntimeException( "Not supported" );
        }


        @Override
        public Map<Long, AlgDataType> getParameterTypes() {
            return dataContext.getParameterTypes();
        }


        @Override
        public void setParameterTypes( Map<Long, AlgDataType> types ) {
            throw new GenericRuntimeException( "Not supported" );
        }

    }

}
