/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.workflow.dag.activities.impl.extract;

import static org.polypheny.db.workflow.dag.activities.impl.extract.DocExtractTextActivity.MAX_STRING_LENGTH;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.util.Source;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.DocType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.EnumSetting;
import org.polypheny.db.workflow.dag.annotations.FileSetting;
import org.polypheny.db.workflow.dag.annotations.IntSetting;
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.FileValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@ActivityDefinition(type = "docExtractText", displayName = "Extract Text", categories = { ActivityCategory.EXTRACT, ActivityCategory.DOCUMENT, ActivityCategory.EXTERNAL },
        inPorts = {},
        outPorts = { @OutPort(type = PortType.DOC, description = "The extracted collection.") },
        shortDescription = "Extracts a collection from one or multiple text files with limited knowledge about their structure.")
@FileSetting(key = "file", displayName = "File Location", pos = 0,
        multi = true,
        shortDescription = "Select the location of the file(s) to extract. In case of multiple files, the union of their documents is computed.")
@EnumSetting(key = "split", displayName = "Splitting Strategy", pos = 1,
        options = { "none", "chunks", "char", "whitespace", "newline", "custom" },
        displayOptions = { "No Splitting", "Fixed Size Chunks", "Individual Characters", "Split Words (Whitespace)", "Split Lines", "Custom Split Pattern" },
        defaultValue = "newline",
shortDescription = "The strategy that determines how the input text is split into chunks. Each chunk becomes its own document.")
@StringSetting(key = "custom", displayName = "Custom Regex Pattern", pos = 2,
        subPointer = "split", subValues = { "\"custom\"" }, maxLength = 100)
@IntSetting(key = "size", displayName = "Chunk Size in Bytes", pos = 3,
        subPointer = "split", subValues = { "\"chunks\"" }, defaultValue = 1000,
        min = 1, max = MAX_STRING_LENGTH) // max document size is 16MB
@BoolSetting(key = "nameField", displayName = "Add File Name Field", pos = 4)

@SuppressWarnings("unused")
public class DocExtractTextActivity implements Activity, Pipeable {
    static final int MAX_STRING_LENGTH = 16_000_000;
    private final PolyString nameField = PolyString.of( "fileName" );
    private final PolyString dataField = PolyString.of( "data" );


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        if ( settings.keysPresent( "nameField" ) && settings.getBool( "nameField" ) ) {
            return DocType.of( Set.of( nameField.value ) ).asOutTypes();
        }
        return DocType.of().asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        Pipeable.super.execute( inputs, settings, ctx );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return getDocType();
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        boolean addNameField = settings.getBool( "nameField" );
        FileValue file = settings.get( "file", FileValue.class );
        String splitMode = settings.getString( "split" );
        String custom = settings.getString( "custom" );
        int size = settings.getInt( "size" );

        for ( Source source : file.getSources() ) {
            if ( !writeDocuments( output, source, addNameField, splitMode, custom, size, ctx ) ) {
                return;
            }
        }
    }


    private boolean writeDocuments( OutputPipe output, Source source, boolean addNameField, String splitMode, String custom, int size, PipeExecutionContext ctx ) throws Exception {
        String name = ActivityUtils.resourceNameFromSource( source );
        PolyString fileName = PolyString.of( name );
        ctx.logInfo( "Extracting " + name );

        try ( InputStream stream = source.openStream() ) {
            switch ( splitMode ) {
                case "none" -> {
                    String str = new String( stream.readAllBytes(), StandardCharsets.UTF_8 );
                    return writeDoc( str, output, addNameField, fileName );
                }
                case "char" -> {
                    try ( BufferedReader reader = new BufferedReader( new InputStreamReader( stream, StandardCharsets.UTF_8 ) ) ) {
                        Iterator<String> chars = readerToCharIterator(reader);
                        while (chars.hasNext()) {
                            if ( !writeDoc( chars.next(), output, addNameField, fileName ) ) {
                                return false;
                            }
                        }
                    }
                }
                case "whitespace", "custom" -> {
                    String delimiter = splitMode.equals( "custom" ) ? custom : "\\s+";
                    try ( Scanner scanner = new Scanner( new InputStreamReader( stream, StandardCharsets.UTF_8 ) ) ) {
                        scanner.useDelimiter( delimiter );
                        while ( scanner.hasNext() ) {
                            if ( !writeDoc( scanner.next(), output, addNameField, fileName ) ) {
                                return false;
                            }
                        }
                    }
                }
                case "newline" -> {
                    try ( BufferedReader reader = new BufferedReader( new InputStreamReader( stream, StandardCharsets.UTF_8 ) ) ) {
                        String line;
                        while ( (line = reader.readLine()) != null ) {
                            if ( !writeDoc( line, output, addNameField, fileName ) ) {
                                return false;
                            }
                        }
                    }
                }
                case "chunks" -> {
                    try ( BufferedReader reader = new BufferedReader( new InputStreamReader( stream, StandardCharsets.UTF_8 ) ) ) {
                        char[] buffer = new char[size];
                        int bytesRead;
                        while ((bytesRead = reader.read(buffer)) != -1) {
                            String chunk = new String(buffer, 0, bytesRead);
                            if (!writeDoc(chunk, output, addNameField, fileName )) {
                                return false;
                            }
                        }
                    }
                }
            }
        }
        return true;
    }


    private boolean writeDoc( String str, OutputPipe output, boolean addNameField, PolyString fileName ) throws InterruptedException {
        if ( str == null ) {
            return true;
        }
        if (str.length() > MAX_STRING_LENGTH) {
            throw new GenericRuntimeException( "String is too long: " + str.length() );
        }
        PolyDocument doc = new PolyDocument();
        doc.put( dataField, PolyString.of( str ) );
        if ( addNameField ) {
            doc.put( nameField, fileName );
        }
        ActivityUtils.addDocId( doc );
        return output.put( doc );
    }


    private Iterator<String> readerToCharIterator( Reader reader ) {
        // A single Unicode character might correspond to multiple chars -> map to string instead
        return IntStream.generate( () -> {
                    try {
                        return reader.read();
                    } catch ( IOException e ) {
                        throw new UncheckedIOException( e );
                    }
                } )
                .takeWhile( cp -> cp != -1 )
                .mapToObj( Character::toChars )
                .map( String::new )
                .iterator();
    }


    @Override
    public long estimateTupleCount( List<AlgDataType> inTypes, Settings settings, List<Long> inCounts, Supplier<Transaction> transactionSupplier ) {
        if ( settings.getString( "split" ).equals( "none" ) ) {
            return 1;
        }
        return 1_000_000; // an arbitrary estimate to show some progress for larger files
    }

}
