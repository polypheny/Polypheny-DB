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

package org.polypheny.db.adapter.parquet.shared.io;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.polypheny.db.adapter.RelationalDataSource.ExportedColumn;
import org.polypheny.db.adapter.parquet.shared.schema.ParquetNameNormalizer;
import org.polypheny.db.adapter.parquet.shared.schema.ParquetTypeConverter;
import org.polypheny.db.adapter.parquet.shared.util.HadoopConfigurationFactory;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.util.Source;

/**
 * Reads parquet file schema and stores metadata
 */
public class ParquetSchemaReader {

    private final List<Source> sources;
    private final List<ParquetMetadata> metadata;
    @Getter
    private final MessageType schema;


    public ParquetSchemaReader( Source source ) {
        this( List.of( source ) );
    }


    public ParquetSchemaReader( List<Source> sources ) {
        if ( sources == null || sources.isEmpty() ) {
            throw new GenericRuntimeException( "Cannot inspect parquet schema without source files." );
        }
        this.sources = sources;
        this.metadata = sources.stream().map( ParquetSchemaReader::readFooter ).toList();
        this.schema = consolidateSchema( metadata );
    }


    private static ParquetMetadata readFooter( Source source ) {
        try {
            URI uri = source.isFile() ? source.file().toURI() : source.url().toURI();
            Path path = new Path( uri );
            Configuration conf = HadoopConfigurationFactory.create( ParquetSourceReader.class.getClassLoader() );
            var inputFile = HadoopInputFile.fromPath( path, conf );
            try ( var schemaReader = ParquetFileReader.open( inputFile ) ) {
                return schemaReader.getFooter();
            }
        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Unable to inspect parquet schema for " + source.path(), e );
        }
    }


    private static MessageType consolidateSchema( List<ParquetMetadata> footers ) {
        Map<String, Type> fields = new LinkedHashMap<>();
        String schemaName = footers.get( 0 ).getFileMetaData().getSchema().getName();
        for ( ParquetMetadata footer : footers ) {
            for ( Type field : footer.getFileMetaData().getSchema().getFields() ) {
                fields.putIfAbsent( field.getName(), field );
            }
        }
        return new MessageType( schemaName, new ArrayList<>( fields.values() ) );
    }


    private static int[] buildProjectedFields( int[] projectedFields, int schemaFieldCount ) {
        if ( projectedFields == null || projectedFields.length == 0 ) {
            int[] allFields = new int[schemaFieldCount];
            for ( int i = 0; i < schemaFieldCount; i++ ) {
                allFields[i] = i;
            }
            return allFields;
        }
        return projectedFields;
    }


    public List<ParquetMetadata> getFooters() {
        return metadata;
    }


    public long getEstimatedRowCount() {
        return metadata.stream()
                .flatMap( footer -> footer.getBlocks().stream() )
                .mapToLong( BlockMetaData::getRowCount )
                .sum();
    }


    public List<ExportedColumn> exportedColumns( String tableName ) {
        try {
            ParquetTypeConverter typeConverter = new ParquetTypeConverter();
            List<ExportedColumn> columns = new ArrayList<>();
            List<Type> fields = getSchema().getFields();
            for ( int i = 0; i < fields.size(); i++ ) {
                Type field = fields.get( i );
                columns.add(
                        new ExportedColumn(
                                ParquetNameNormalizer.normalizeFieldName( field.getName() ),
                                typeConverter.fromParquetTypeToPolyType( field ),
                                null,
                                null,
                                null,
                                null,
                                null,
                                false,
                                tableName,
                                tableName,
                                field.getName(),
                                i,
                                i == 0
                        )
                );
            }
            return columns;
        } catch ( Exception e ) {
            throw new GenericRuntimeException( e );
        }
    }


    public MessageType buildProjectionSchema( int[] fields ) {
        int[] projectedFields = buildProjectedFields( fields, schema.getFieldCount() );
        if ( projectedFields.length == 0 ) {
            return schema;
        }

        List<Type> types = new ArrayList<>( projectedFields.length );
        for ( int index : projectedFields ) {
            types.add( schema.getType( index ) );
        }
        return new MessageType( schema.getName(), types );
    }


    @Override
    public String toString() {
        return String.join( ", ", sources.stream().map( Source::path ).toList() );
    }

}
