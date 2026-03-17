/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.adapter.parquet.execution;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.polypheny.db.adapter.parquet.model.ParquetFilter;
import org.polypheny.db.adapter.parquet.schema.ParquetTypeConverter;
import org.polypheny.db.adapter.parquet.util.HadoopConfigurationFactory;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Source;

public class ParquetEnumerator implements Enumerator<PolyValue[]> {

    private final AtomicBoolean cancelFlag;
    private final List<ParquetFilter> filters;
    private final ParquetFileReader fileReader;
    private final MessageType schema;
    private final MessageType projectionSchema;
    private final int[] projectionIndexes;
    private final List<BlockMetaData> blocks;
    private final ValueExtractor valueExtractor;
    private final PredicateEvaluator predicateEvaluator;
    private final ParquetTypeConverter typeConverter;
    private final int columnCount;

    private RecordReader<Group> recordReader;
    private long rowCountInGroup;
    private long rowIndexInGroup;
    private int blockIndex;
    private PolyValue[] current;


    /**
     * Constructor
     * @param source file
     * @param cancelFlag used in moveNext()
     * @param fields columns
     * @param filters list of parquet filters
     * @throws GenericRuntimeException on failure
     */

    public ParquetEnumerator(
            Source source,
            AtomicBoolean cancelFlag,
            int[] fields,
            List<ParquetFilter> filters ) throws GenericRuntimeException {

        this.cancelFlag = cancelFlag;
        this.filters = filters == null ? List.of() : filters;
        this.valueExtractor = new ValueExtractor();
        this.predicateEvaluator = new PredicateEvaluator();
        this.typeConverter = new ParquetTypeConverter();

        try {
            // create file reader
            Path path = new Path( source.url().toURI() );
            Configuration conf = HadoopConfigurationFactory.create( this.getClass().getClassLoader() );
            this.fileReader = ParquetFileReader.open( HadoopInputFile.fromPath( path, conf ) );

            this.schema = fileReader.getFooter().getFileMetaData().getSchema();
            this.columnCount = fields.length == 0 ? schema.getFieldCount() : Math.min( fields.length, schema.getFieldCount() );
            // combine projection columns (fields) with filter columns
            this.projectionIndexes = computeProjectionIndexes( fields, this.filters, this.columnCount );
            this.projectionSchema = buildProjectionSchema( schema, projectionIndexes );
            this.fileReader.setRequestedSchema( projectionSchema );
            this.blocks = fileReader.getFooter().getBlocks();

            this.blockIndex = 0;
            this.recordReader = null;
            this.rowCountInGroup = 0;
            this.rowIndexInGroup = 0;
        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Unable to open parquet file: " + source.path(), e );
        }
    }

    //region Enumerator operations


    /**
     * return current row
     *
     * @return current row
     */
    @Override
    public PolyValue[] current() {
        return current;
    }


    /**
     * Move to the next matching row
     *
     * @return True if there is a new row or False otherwise.
     */
    @Override
    public boolean moveNext() {
        try {
            for ( ; ; ) {
                if ( cancelFlag.get() ) {
                    current = null;
                    return false;
                }

                if ( !ensureRecordReader() ) {
                    current = null;
                    return false;
                }

                Group group = recordReader.read();
                rowIndexInGroup++;
                if ( group == null ) {
                    recordReader = null;
                    continue;
                }

                PolyValue[] row = extractRow( group );
                if ( !matchesFilters( row ) ) {
                    continue;
                }

                current = row;
                return true;
            }
        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Error while reading parquet data", e );
        }
    }


    /**
     * Reset is not supported
     */
    @Override
    public void reset() {
        throw new UnsupportedOperationException();
    }


    /**
     * Close reader
     */
    @Override
    public void close() {
        try {
            fileReader.close();
        } catch ( IOException e ) {
            throw new GenericRuntimeException( "Error closing parquet reader", e );
        }
    }
    //endregion


    /**
     * Ensures a record reader is available for the current row group.
     *
     * @return true if reader was created, false - if no compatible row groups found
     * @throws IOException when read fails
     */
    private boolean ensureRecordReader() throws IOException {
        if ( recordReader != null && rowIndexInGroup < rowCountInGroup ) {
            return true;
        }

        while ( blockIndex < blocks.size() ) {
            BlockMetaData block = blocks.get( blockIndex );
            // check if block contains filter values according to metadata in block
            // if not - row group can be skipped
            if ( !predicateEvaluator.mightContain( block, schema, filters ) ) {
                fileReader.skipNextRowGroup();
                blockIndex++;
                continue;
            }

            // create reader for compatible row group

            PageReadStore pages = fileReader.readNextRowGroup();
            blockIndex++;

            if ( pages == null ) {
                return false;
            }

            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO( this.projectionSchema );
            this.recordReader = columnIO.getRecordReader( pages, new GroupRecordConverter( this.projectionSchema ) );
            this.rowCountInGroup = pages.getRowCount();
            this.rowIndexInGroup = 0;
            return true;
        }

        return false;
    }


    /**
     * Extracts one row using the requested read schema.
     */
    private PolyValue[] extractRow( Group group ) {
        PolyValue[] row = new PolyValue[columnCount];
        for ( int readIndex = 0; readIndex < this.projectionIndexes.length; readIndex++ ) {
            var type = projectionSchema.getType( readIndex );
            var value = valueExtractor.extractValue( group, readIndex, type );
            row[readIndex] = typeConverter.fromObjToPolyValue( type, value );
        }
        return row;
    }


    /**
     * Evaluates pushed filters against one extracted row.
     */
    private boolean matchesFilters( PolyValue[] row ) {
        if ( filters.isEmpty() ) {
            return true;
        }

        // each filter - for specific column
        for ( ParquetFilter filter : filters ) {
            // validate column index
            int idx = filter.columnIndex();
            if ( idx < 0 || idx >= row.length || idx >= projectionIndexes.length ) {
                return false; //invalid filter column
            }

            // apply filter only for primitive types
            int schemaFieldIndex = projectionIndexes[idx];
            Type fieldType = schema.getType( schemaFieldIndex );
            if ( !fieldType.isPrimitive() ) {
                return false;
            }

            Object actual = row[idx];
            if ( actual == null ) {
                return false;
            }

            PrimitiveType primitiveType = fieldType.asPrimitiveType();
            Object expected = this.typeConverter.fromLiteralToPrimitive( primitiveType, filter.literalValue() );
            if ( expected == null ) {
                // incorrect filter value
                return false;
            }

            Integer cmp = ValueComparator.compareValues( primitiveType, actual, expected );

            if ( cmp == null || !matchesOperator( filter, cmp ) ) {
                return false;
            }
        }
        return true;
    }


    /**
     * Applies comparison result to a filter operator.
     */
    private boolean matchesOperator( ParquetFilter filter, int cmp ) {
        return switch ( filter.operator() ) {
            case EQUALS -> cmp == 0;
            case NOT_EQUALS -> cmp != 0;
            case GREATER_THAN -> cmp > 0;
            case GREATER_THAN_OR_EQUAL -> cmp >= 0;
            case LESS_THAN -> cmp < 0;
            case LESS_THAN_OR_EQUAL -> cmp <= 0;
            default -> false;
        };
    }


    /**
     * Computes the set of columns that must be read:
     * combines projection columns (fields) with filter columns
     */
    private static int[] computeProjectionIndexes( int[] projectedFields, List<ParquetFilter> filters, int columnCount ) {
        Set<Integer> fields = new LinkedHashSet<>();
        for ( int field : projectedFields ) {
            if ( field >= 0 && field < columnCount ) {
                fields.add( field );
            }
        }
        for ( ParquetFilter filter : filters ) {
            int index = filter.columnIndex();
            if ( index >= 0 && index < columnCount ) {
                fields.add( index );
            }
        }
        return fields.stream().mapToInt( Integer::intValue ).toArray();
    }


    /**
     * Builds the projected read schema.
     */
    private static MessageType buildProjectionSchema( MessageType fullSchema, int[] projectedFieldIndexes ) {
        if ( projectedFieldIndexes.length == 0 ) {
            return fullSchema;
        }

        List<Type> fields = new ArrayList<>( projectedFieldIndexes.length );
        for ( int index : projectedFieldIndexes ) {
            fields.add( fullSchema.getType( index ) );
        }
        return new MessageType( fullSchema.getName(), fields );
    }


}
