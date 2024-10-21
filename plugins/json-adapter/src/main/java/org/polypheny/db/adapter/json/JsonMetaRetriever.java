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

package org.polypheny.db.adapter.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.polypheny.db.adapter.DocumentDataSource.ExportedDocument;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.util.Source;
import org.polypheny.db.util.Sources;

final class JsonMetaRetriever {

    static List<ExportedDocument> getDocuments( URL jsonFiles ) throws IOException {
        List<ExportedDocument> exportedDocuments = new LinkedList<>();
        Set<String> fileNames = getFileNames( jsonFiles );
        ObjectMapper objectMapper = new ObjectMapper();
        JsonFactory jsonFactory = new JsonFactory( objectMapper );

        for ( String fileName : fileNames ) {
            URL jsonFile = new URL( jsonFiles, fileName );
            try ( InputStream inputStream = jsonFile.openStream();
                    JsonParser jsonParser = jsonFactory.createParser( inputStream ) ) {
                String entityName = deriveEntityName( jsonFile.getFile() );
                JsonToken token = jsonParser.nextToken();
                if ( token == JsonToken.START_ARRAY || token == JsonToken.START_OBJECT ) {
                    exportedDocuments.add( new ExportedDocument( entityName, false, EntityType.SOURCE ) );
                } else {
                    throw new GenericRuntimeException( "JSON file does not contain a valid top-level structure (neither an object nor an array)" );
                }
            }
        }
        return exportedDocuments;
    }


    static URL findDocumentUrl( URL jsonFiles, String name ) throws MalformedURLException, NoSuchFileException {
        String[] extensions = { ".json", ".json.gz" };
        String path = jsonFiles.getPath();

        // handle single file
        for ( String ext : extensions ) {
            if ( path.endsWith( name + ext ) ) {
                return jsonFiles;
            }
        }

        // handle directory
        Set<String> fileNames = getFileNames( jsonFiles );
        for ( String file : fileNames ) {
            for ( String ext : extensions ) {
                if ( file.equals( name + ext ) ) {
                    return new URL( jsonFiles, file );
                }
            }
        }

        throw new NoSuchFileException( "No JSON file(s) found under the URL '" + jsonFiles + "'" );
    }


    private static Set<String> getFileNames( URL jsonFiles ) throws NoSuchFileException {
        Source source = Sources.of( jsonFiles );
        if ( source.isFile() ) {
            File file = source.file();
            if ( file.isFile() ) {
                // url is file
                return Set.of( file.getName() );
            }

            // url is directory
            File[] files = file.listFiles( ( d, name ) -> name.endsWith( ".json" ) );
            if ( files == null || files.length == 0 ) {
                throw new NoSuchFileException( "No .json files were found." );
            }
            return Arrays.stream( files )
                    .map( File::getName )
                    .collect( Collectors.toSet() );
        }
        // url is web source
        String filePath = jsonFiles.getPath();
        return Set.of( filePath.substring( filePath.lastIndexOf( '/' ) + 1 ) );
    }


    private static String deriveEntityName( String fileName ) {
        fileName = fileName.replaceAll( "/+$", "" );  // remove trailing "/"
        return fileName
                .substring( fileName.lastIndexOf( '/' ) + 1 )  // extract file name after last "/"
                .toLowerCase()
                .replace( ".json.gz", "" )
                .replace( ".json", "" )
                .replaceAll( "[^a-z0-9_]+", "" )  // remove invalid characters
                .trim();
    }

}
