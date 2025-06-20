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

import static org.polypheny.db.workflow.dag.settings.GroupDef.ADVANCED_GROUP;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.InputStream;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import org.polypheny.db.algebra.type.AlgDataType;
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
import org.polypheny.db.workflow.dag.annotations.FileSetting;
import org.polypheny.db.workflow.dag.annotations.IntSetting;
import org.polypheny.db.workflow.dag.settings.FileValue;
import org.polypheny.db.workflow.dag.settings.FileValue.SourceType;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@ActivityDefinition(type = "docExtractJson", displayName = "Extract JSON", categories = { ActivityCategory.EXTRACT, ActivityCategory.DOCUMENT, ActivityCategory.ESSENTIALS, ActivityCategory.EXTERNAL },
        inPorts = {},
        outPorts = { @OutPort(type = PortType.DOC, description = "The extracted collection.") },
        shortDescription = "Extracts a collection from one or multiple JSON files stored locally or remotely. The file can either contain a single object, an array of objects or one object per line.")
@FileSetting(key = "file", displayName = "File Location", pos = 0,
        multi = true, modes = { SourceType.ABS_FILE, SourceType.URL },
        shortDescription = "Select the location of the file(s) to extract. In case of multiple files, the union of their documents is computed.")
@BoolSetting(key = "nameField", displayName = "Add File Name Field", pos = 1)
@IntSetting(key = "maxCount", displayName = "Maximum Document Count", defaultValue = -1, min = -1, pos = 2, group = ADVANCED_GROUP,
        shortDescription = "The maximum number of documents to extract per file or -1 to extract all.")

@SuppressWarnings("unused")
public class DocExtractJsonActivity implements Activity, Pipeable {

    private static final ObjectMapper mapper = new ObjectMapper();
    public static final PolyString NAME_FIELD = PolyString.of( "fileName" );
    private static final Set<String> EXTENSIONS = Set.of( "json", "ndjson", "geojson", "txt", "jso", "ldjson", "jsonl" );


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        if ( settings.keysPresent( "nameField" ) && settings.getBool( "nameField" ) ) {
            return DocType.of( Set.of( NAME_FIELD.value ) ).asOutTypes();
        }
        return DocType.of().asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        Pipeable.super.execute( inputs, settings, ctx );
    }


    @Override
    public String getDynamicName( List<TypePreview> inTypes, SettingsPreview settings ) {
        if ( settings.keysPresent( "file" ) ) {
            FileValue file = settings.getOrThrow( "file", FileValue.class );
            try {
                List<Source> sources = file.getSources( EXTENSIONS );
                if ( sources.size() > 1 ) {
                    return "Extract JSONs";
                } else if ( sources.size() == 1 ) {
                    String name = ActivityUtils.resourceNameFromSource( sources.get( 0 ) );
                    if ( name.length() > 40 ) {
                        name = name.substring( 0, 37 ) + "...";
                    }
                    return "Extract JSON: " + name;
                }

            } catch ( Exception ignored ) {
            }
        }
        return null;
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return getDocType();
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        boolean addNameField = settings.getBool( "nameField" );
        FileValue file = settings.get( "file", FileValue.class );
        int maxCount = settings.getInt( "maxCount" );
        for ( Source source : file.getSources( EXTENSIONS ) ) {
            if ( !writeDocuments( output, source, addNameField, maxCount, ctx ) ) {
                return;
            }
        }
    }


    private boolean writeDocuments( OutputPipe output, Source source, boolean addNameField, int maxCount, PipeExecutionContext ctx ) throws Exception {
        String name = ActivityUtils.resourceNameFromSource( source );
        PolyString polyName = PolyString.of( name );
        ctx.logInfo( "Extracting " + name );
        long docCount = 0;
        try ( InputStream stream = source.openStream() ) {
            JsonParser parser = mapper.getFactory().createParser( stream );
            JsonToken firstToken = parser.nextToken();
            if ( firstToken == JsonToken.START_ARRAY ) {
                while ( parser.nextToken() == JsonToken.START_OBJECT ) {
                    if ( maxCount >= 0 && docCount++ == maxCount ) {
                        return true;
                    }
                    ObjectNode node = mapper.readTree( parser );
                    PolyDocument doc = DocCreateActivity.getDocument( node );
                    if ( addNameField ) {
                        doc.put( NAME_FIELD, polyName );
                    }
                    if ( !output.put( doc ) ) {
                        return false;
                    }
                }
            } else {
                if ( firstToken != JsonToken.START_OBJECT ) {
                    throw new IllegalStateException( "Unexpected JSON format: must start with [ or {" );
                }
                MappingIterator<ObjectNode> iterator = mapper.readerFor( ObjectNode.class ).readValues( parser );
                while ( iterator.hasNext() ) {
                    if ( maxCount >= 0 && docCount++ == maxCount ) {
                        return true;
                    }
                    ObjectNode node = iterator.next();
                    PolyDocument doc = DocCreateActivity.getDocument( node );
                    if ( addNameField ) {
                        doc.put( NAME_FIELD, polyName );
                    }
                    if ( !output.put( doc ) ) {
                        return false;
                    }
                }
            }
        }
        return true;
    }


    @Override
    public long estimateTupleCount( List<AlgDataType> inTypes, Settings settings, List<Long> inCounts, Supplier<Transaction> transactionSupplier ) {
        return 1_000_000; // an arbitrary estimate to show some progress for larger files
    }

}
