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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.polypheny.db.adapter.parquet.shared.util.HadoopConfigurationFactory;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.util.Source;

/**
 * Reads parquet file schema and stores metadata
 */
public class ParquetSchemaReader {

    private final ParquetMetadata metadata;


    public ParquetSchemaReader( Source source ) {
        metadata = readFooter( source );
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


    public MessageType getSchema() {
        return metadata.getFileMetaData().getSchema();
    }

    public ParquetMetadata getFooter() {
        return metadata;
    }


    public long getEstimatedRowCount() {
        return metadata.getBlocks().stream().mapToLong( BlockMetaData::getRowCount ).sum();
    }

}
