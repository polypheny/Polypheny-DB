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
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.polypheny.db.adapter.DocumentDataSource.ExportedDocument;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.util.Sources;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

public class XmlMetaRetriever {

    public static List<ExportedDocument> getDocuments( URL xmlFiles ) throws IOException, ParserConfigurationException, SAXException {
        List<ExportedDocument> exportedDocuments = new LinkedList<>();
        Set<String> fileNames = getFileNames( xmlFiles );
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        for ( String fileName : fileNames ) {
            URL xmlFile = new URL( xmlFiles, fileName );
            // TODO: adapt to handle more diverse document structures
            Document document = builder.parse( xmlFile.openStream() );
            document.getDocumentElement().normalize();
            String entityName = deriveEntityName( xmlFile.getFile() );
            Node rootNode = document.getDocumentElement();
            if ( rootNode.getNodeType() != Node.ELEMENT_NODE ) {
                throw new RuntimeException( "XML file does not contain a valid top-level element node" );
            }
            exportedDocuments.add( new ExportedDocument( entityName, false, EntityType.SOURCE ) );
        }
        return exportedDocuments;
    }


    public static URL findDocumentUrl( URL xmlFiles, String name ) throws MalformedURLException, NoSuchFileException {
        String[] extensions = { ".xml", ".xml.gz" };
        String path = xmlFiles.getPath();

        for ( String ext : extensions ) {
            if ( path.endsWith( name + ext + "/" ) ) {
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


    private static Set<String> getFileNames( URL xmlFiles ) {
        Set<String> fileNames;
        if ( Sources.of( xmlFiles ).file().isFile() ) {
            fileNames = Set.of( xmlFiles.getPath() );
            return fileNames;
        }
        File[] files = Sources.of( xmlFiles )
                .file()
                .listFiles( ( d, name ) -> name.endsWith( ".xml" ) );
        if ( files == null ) {
            throw new GenericRuntimeException( "No .xml files where found." );
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
                .replace( ".xml.gz", "" )
                .replace( ".xml", "" )
                .replaceAll( "[^a-z0-9_]+", "" )  // remove invalid characters
                .trim();
    }

}
