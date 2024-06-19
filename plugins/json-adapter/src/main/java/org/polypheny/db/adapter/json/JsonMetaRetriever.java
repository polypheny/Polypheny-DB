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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
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

public class JsonMetaRetriever {

    public static List<ExportedDocument> getDocuments( URL jsonFiles ) throws IOException {
        List<ExportedDocument> exportedDocuments = new LinkedList<>();
        Set<String> fileNames = getFileNames( jsonFiles );
        for (String fileName : fileNames) {
            URL jsonFile = new URL( jsonFiles, fileName );
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode rootNode = objectMapper.readTree( jsonFile );
            String entityName = deriveEntityName( jsonFile.getFile() );
            if ( !(rootNode.isArray() || rootNode.isObject()) ) {
                throw new RuntimeException( "JSON file does not contain a valid top-level structure (neither an object nor an array)" );
            }
            exportedDocuments.add( new ExportedDocument( entityName, false, EntityType.SOURCE ) );
        }
        return exportedDocuments;
    }

    public static URL findDocumentUrl(URL jsonFiles, String name) throws MalformedURLException {
        Set<String> fileNames = getFileNames( jsonFiles );
        String[] extensions = {".json", ".json.gz"};
        for (String file : fileNames) {
            for (String ext : extensions) {
                if (file.equals(name + ext)) {
                    return new URL (jsonFiles, file);
                }
            }
        }

        // Return null or throw an exception if file not found
        return null;
    }


    private static Set<String> getFileNames( URL jsonFiles ) {
        Set<String> fileNames;
        if ( Sources.of( jsonFiles ).file().isFile() ) {
            fileNames = Set.of( jsonFiles.getPath() );
            return fileNames;
        }
        File[] files = Sources.of( jsonFiles )
                .file()
                .listFiles( ( d, name ) -> name.endsWith( ".json" ));
        if ( files == null ) {
            throw new GenericRuntimeException( "No .json files where found." );
        }
        fileNames = Arrays.stream( files )
                .sequential()
                .map( File::getName )
                .collect( Collectors.toSet() );
        return fileNames;
    }


    private static String deriveEntityName( String fileName ) {
        fileName = fileName.replaceAll( "/+$", "" );  // remove trailing "/"
        return fileName
                .substring( fileName.lastIndexOf( '/' ) + 1 )  // extract file name after last "/"
                .toLowerCase()
                .replace( ".json", "" )
                .replaceAll( "[^a-z0-9_]+", "" )  // remove invalid characters
                .trim();
    }

}
