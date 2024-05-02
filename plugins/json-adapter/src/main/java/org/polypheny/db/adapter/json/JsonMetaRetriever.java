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
import java.io.IOException;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.polypheny.db.adapter.DocumentDataSource.ExportedDocument;
import org.polypheny.db.catalog.logistic.EntityType;

public class JsonMetaRetriever {

    public static List<ExportedDocument> getDocuments( URL jsonFile ) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree( jsonFile );
        List<ExportedDocument> exportedDocuments = new LinkedList<>();
        String entityName = deriveEntityName( jsonFile.getFile() );
        if ( rootNode.isArray() ) {
            AtomicInteger enumerator = new AtomicInteger();
            rootNode.forEach( elementNode -> exportedDocuments.add( new ExportedDocument( entityName + enumerator.getAndIncrement(), false, EntityType.SOURCE ) ) );
        } else if ( rootNode.isObject() ) {
            exportedDocuments.add( new ExportedDocument( entityName, false, EntityType.SOURCE ) );
        } else {
            throw new RuntimeException( "JSON file does not contain a valid top-level structure (neither an object nor an array)" );
        }
        return exportedDocuments;
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
