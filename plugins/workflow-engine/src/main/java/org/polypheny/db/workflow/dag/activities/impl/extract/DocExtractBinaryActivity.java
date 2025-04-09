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
import static org.polypheny.db.workflow.dag.activities.impl.extract.DocExtractTextActivity.dataField;
import static org.polypheny.db.workflow.dag.activities.impl.extract.DocExtractTextActivity.nameField;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyBinary;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.numerical.PolyInteger;
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
import org.polypheny.db.workflow.dag.settings.EnumSettingDef.EnumStyle;
import org.polypheny.db.workflow.dag.settings.FileValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@ActivityDefinition(type = "docExtractBinary", displayName = "Extract Binary Data", categories = { ActivityCategory.EXTRACT, ActivityCategory.DOCUMENT, ActivityCategory.EXTERNAL },
        inPorts = {},
        outPorts = { @OutPort(type = PortType.DOC, description = "The extracted collection.") },
        shortDescription = "Extracts a collection from one or multiple files with limited knowledge about their structure. If the file contains textual data, the 'Extract Text' activity is better suited.")
@FileSetting(key = "file", displayName = "File Location", pos = 0,
        multi = true,
        shortDescription = "Select the location of the file(s) to extract. In case of multiple files, the union of their documents is computed.")
@EnumSetting(key = "split", displayName = "Splitting Strategy", pos = 1,
        options = { "none", "chunks" },
        displayOptions = { "No Splitting", "Fixed Size Chunks" },
        defaultValue = "chunks", style = EnumStyle.RADIO_BUTTON,
        shortDescription = "The strategy that determines how the input is split into chunks. Each chunk becomes its own document.")
@IntSetting(key = "size", displayName = "Chunk Size in Bytes", pos = 3,
        subPointer = "split", subValues = { "\"chunks\"" }, defaultValue = 1000,
        min = 1, max = MAX_STRING_LENGTH) // max document size is 16MB
@EnumSetting(key = "encoding", displayName = "Output Encoding", pos = 4,
        options = { "raw", "base64", "array" },
        displayOptions = { "Raw", "Base64", "Array of Numbers" },
        defaultValue = "base64", style = EnumStyle.RADIO_BUTTON,
        shortDescription = "How the binary data is encoded for storage in a document.")
@BoolSetting(key = "nameField", displayName = "Add File Name Field", pos = 5)

@SuppressWarnings("unused")
public class DocExtractBinaryActivity implements Activity, Pipeable {


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
        String encoding = settings.getString( "encoding" );
        int size = settings.getInt( "size" );

        for ( Source source : file.getSources() ) {
            if ( !writeDocuments( output, source, addNameField, splitMode, size, encoding, ctx ) ) {
                return;
            }
        }
    }


    private boolean writeDocuments( OutputPipe output, Source source, boolean addNameField, String splitMode, int size, String encoding, PipeExecutionContext ctx ) throws Exception {
        String name = ActivityUtils.resourceNameFromSource( source );
        PolyString fileName = PolyString.of( name );
        ctx.logInfo( "Extracting " + name );

        try ( InputStream stream = source.openStream() ) {
            switch ( splitMode ) {
                case "none" -> {
                    return writeDoc( stream.readAllBytes(), encoding, output, addNameField, fileName );
                }
                case "chunks" -> {
                    byte[] buffer = new byte[size];
                    int bytesRead;
                    while ( (bytesRead = stream.read( buffer )) != -1 ) {
                        byte[] chunk = Arrays.copyOf( buffer, bytesRead ); // we need a copy to ensure the array is not overwritten
                        if ( !writeDoc( chunk, encoding, output, addNameField, fileName ) ) {
                            return false;
                        }
                    }
                }
            }
        }
        return true;
    }


    private boolean writeDoc( byte[] data, String encoding, OutputPipe output, boolean addNameField, PolyString fileName ) throws InterruptedException {
        if ( data == null || data.length == 0 ) {
            return true;
        }
        if ( data.length > MAX_STRING_LENGTH ) {
            throw new GenericRuntimeException( "Binary data is too long: " + data.length );
        }
        PolyValue value = switch ( encoding ) {
            case "raw" -> PolyBinary.of( data );
            case "base64" -> PolyString.of( Base64.getEncoder().encodeToString( data ) );
            case "array" -> {
                PolyList<PolyValue> list = new PolyList<>();
                for ( byte b : data ) {
                    list.add( PolyInteger.of( b ) );
                }
                yield list;
            }
            default -> throw new GenericRuntimeException( "Unsupported encoding: " + encoding );
        };

        PolyDocument doc = new PolyDocument();
        doc.put( dataField, value );
        if ( addNameField ) {
            doc.put( nameField, fileName );
        }
        ActivityUtils.addDocId( doc );
        return output.put( doc );
    }


    @Override
    public long estimateTupleCount( List<AlgDataType> inTypes, Settings settings, List<Long> inCounts, Supplier<Transaction> transactionSupplier ) {
        if ( settings.getString( "split" ).equals( "none" ) ) {
            return 1;
        }
        return 1_000_000; // an arbitrary estimate to show some progress for larger files
    }

}
