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

package org.polypheny.db.adapter.xml;

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
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.util.Source;
import org.polypheny.db.util.Sources;

final class XmlMetaRetriever {

    static List<ExportedDocument> getDocuments( URL xmlFiles ) throws IOException {
        List<ExportedDocument> exportedDocuments = new LinkedList<>();
        Set<String> fileNames = getFileNames( xmlFiles );

        for ( String fileName : fileNames ) {
            URL xmlFile = new URL( xmlFiles, fileName );
            String entityName = deriveEntityName( xmlFile.getFile() );
            try ( InputStream inputStream = xmlFile.openStream() ) {
                if ( inputStream != null ) {
                    exportedDocuments.add( new ExportedDocument( entityName, false, EntityType.SOURCE ) );
                }
            }
        }
        return exportedDocuments;
    }


    static URL findDocumentUrl( URL xmlFiles, String name ) throws MalformedURLException, NoSuchFileException {
        String[] extensions = { ".xml", ".xml.gz" };
        String path = xmlFiles.getPath();

        for ( String ext : extensions ) {
            if ( path.endsWith( name + ext ) ) {
                return xmlFiles;
            }
        }

        Set<String> fileNames = getFileNames( xmlFiles );
        for ( String file : fileNames ) {
            for ( String ext : extensions ) {
                if ( file.equals( name + ext ) ) {
                    return new URL( xmlFiles, file );
                }
            }
        }

        throw new NoSuchFileException( "No XML file(s) found under the URL '" + xmlFiles + "'" );
    }


    private static Set<String> getFileNames( URL xmlFiles ) throws NoSuchFileException {
        Source source = Sources.of( xmlFiles );
        if ( source.isFile() ) {
            File file = source.file();
            if ( file.isFile() ) {
                // url is file
                return Set.of( file.getName() );
            }

            // url is directory
            File[] files = file.listFiles( ( d, name ) -> name.endsWith( ".xml" ) );
            if ( files == null || files.length == 0 ) {
                throw new NoSuchFileException( "No .xml files were found." );
            }
            return Arrays.stream( files )
                    .map( File::getName )
                    .collect( Collectors.toSet() );
        }
        // url is web source
        String filePath = xmlFiles.getPath();
        return Set.of( filePath.substring( filePath.lastIndexOf( '/' ) + 1 ) );
    }


    private static String deriveEntityName( String fileName ) {
        fileName = fileName.replaceAll( "/+$", "" );  // remove trailing "/"
        return fileName
                .substring( fileName.lastIndexOf( '/' ) + 1 )  // extract file name after last "/"
                .toLowerCase()
                .replace( ".xml.gz", "" )
                .replace( ".xml", "" )
                .replaceAll( "[^a-z0-9_]+", "" )  // remove invalid characters
                .trim();
    }

}
