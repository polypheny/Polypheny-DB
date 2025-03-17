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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.function.Supplier;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.util.Source;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.DocType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.DefaultGroup;
import org.polypheny.db.workflow.dag.annotations.FileSetting;
import org.polypheny.db.workflow.dag.annotations.Group.Subgroup;
import org.polypheny.db.workflow.dag.annotations.IntSetting;
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.FileValue;
import org.polypheny.db.workflow.dag.settings.FileValue.SourceType;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@Slf4j
@ActivityDefinition(type = "docExtractXml", displayName = "Extract XML", categories = { ActivityCategory.EXTRACT, ActivityCategory.DOCUMENT, ActivityCategory.ESSENTIALS, ActivityCategory.EXTERNAL },
        inPorts = {},
        outPorts = { @OutPort(type = PortType.DOC, description = "The extracted collection.") },
        shortDescription = "Extracts a collection from one or multiple XML files.")
@DefaultGroup(subgroups = { @Subgroup(key = "xml", displayName = "Customize Mapping") })

@FileSetting(key = "file", displayName = "File Location", pos = 0,
        multi = true, modes = { SourceType.ABS_FILE, SourceType.URL },
        shortDescription = "Select the location of the file(s) to extract. In case of multiple files, the union of their documents is computed.")
@BoolSetting(key = "nameField", displayName = "Add File Name Field", pos = 1)

@BoolSetting(key = "cast", displayName = "Cast Values", subGroup = "xml", pos = 2,
        defaultValue = true
)
@StringSetting(key = "pointer", displayName = "Target Field", pos = 3,
        maxLength = 1024, subGroup = "xml",
        shortDescription = "The target (sub)field of the mapped XML document. If it is an array of objects, each object becomes its own output document.")
@IntSetting(key = "maxCount", displayName = "Maximum Document Count", defaultValue = -1, min = -1, pos = 4, group = ADVANCED_GROUP,
        shortDescription = "The maximum number of documents to extract per file or -1 to extract all.")

@SuppressWarnings("unused")
public class DocExtractXmlActivity implements Activity, Pipeable {

    private static final String ATTR_PREFIX = "@";
    private static final PolyString TEXT = PolyString.of( "_value" );

    private static final XMLInputFactory xmlFactory = XMLInputFactory.newInstance();
    private static final Set<String> EXTENSIONS = Set.of( "xml", "xhtml", "txt", "config", "rss", "xsd", "svg", "kml", "gpx" );

    private boolean addNameField;
    private boolean cast;
    private String pointer;
    private int maxCount;


    public DocExtractXmlActivity() {
    }


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        if ( settings.keysPresent( "nameField" ) && settings.getBool( "nameField" ) ) {
            return DocType.of( Set.of( DocExtractJsonActivity.NAME_FIELD.value ) ).asOutTypes();
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
                    return "Extract XMLs";
                } else if ( sources.size() == 1 ) {
                    String name = ActivityUtils.resourceNameFromSource( sources.get( 0 ) );
                    if ( name.length() > 40 ) {
                        name = name.substring( 0, 37 ) + "...";
                    }
                    return "Extract XML: " + name;
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
        addNameField = settings.getBool( "nameField" );
        cast = settings.getBool( "cast" );
        pointer = settings.getString( "pointer" );
        FileValue file = settings.get( "file", FileValue.class );
        maxCount = settings.getInt( "maxCount" );
        for ( Source source : file.getSources( EXTENSIONS ) ) {
            if ( !writeDocuments( output, source, ctx ) ) {
                return;
            }
        }
    }


    private boolean writeDocuments( OutputPipe output, Source source, PipeExecutionContext ctx ) throws Exception {
        String name = ActivityUtils.resourceNameFromSource( source );
        PolyString polyName = PolyString.of( name );
        ctx.logInfo( "Extracting " + name );
        long docCount = 0;
        try ( InputStream stream = source.openStream() ) {

            XMLStreamReader reader = xmlFactory.createXMLStreamReader( stream );
            PolyDocument root = toPolyDocument( reader, cast );
            List<PolyDocument> docs = new ArrayList<>();
            if ( !pointer.isEmpty() ) { // TODO: improve performance by resolving pointer during parsing
                PolyValue value;
                try {
                    value = ActivityUtils.getSubValue( root, pointer );
                } catch ( Exception e ) {
                    throw new InvalidSettingException( "Invalid pointer to target field: " + pointer, "pointer" );
                }
                if ( value == null ) {
                    throw new GenericRuntimeException( "Value at " + pointer + " does not exist" );
                } else if ( value.isDocument() ) {
                    docs.add( value.asDocument() );
                } else if ( value.isList() && value.asList().stream().allMatch( PolyValue::isDocument ) && !value.asList().isEmpty() ) {
                    for ( PolyValue entry : value.asList() ) {
                        docs.add( entry.asDocument() );
                    }
                } else {
                    docs.add( new PolyDocument( Map.of( PolyString.of( ActivityUtils.getChildPointer( pointer ) ), value ) ) );
                }
            } else {
                docs.add( root );
            }

            for ( PolyDocument doc : docs ) {
                if ( maxCount >= 0 && docCount++ == maxCount ) {
                    return true;
                }
                if ( addNameField ) {
                    doc.put( DocExtractJsonActivity.NAME_FIELD, polyName );
                }
                if ( !output.put( doc ) ) {
                    return false;
                }
            }
        }
        return true;
    }


    protected static PolyDocument toPolyDocument( XMLStreamReader reader, boolean cast ) throws XMLStreamException {
        PolyDocument rootDoc = new PolyDocument();

        Stack<PolyDocument> contextStack = new Stack<>();
        Stack<PolyString> tagStack = new Stack<>();
        PolyDocument currentDoc = rootDoc;

        while ( reader.hasNext() ) {
            int event = reader.next();
            switch ( event ) {
                case XMLStreamConstants.START_ELEMENT -> {
                    contextStack.push( currentDoc );
                    currentDoc = new PolyDocument();
                    PolyString elementName = PolyString.of( reader.getLocalName() );
                    tagStack.push( elementName );

                    for ( int i = 0; i < reader.getAttributeCount(); i++ ) {
                        String name = reader.getAttributeLocalName( i );
                        String value = reader.getAttributeValue( i );
                        currentDoc.put( PolyString.of( ATTR_PREFIX + name ), getCasted( value, cast ) );
                    }
                }
                case XMLStreamConstants.CHARACTERS -> {
                    String text = reader.getText().trim();
                    if ( !text.isEmpty() ) {
                        putWithDuplicates( currentDoc, TEXT, getCasted( text, cast ) );
                    }
                }
                case XMLStreamConstants.END_ELEMENT -> {
                    PolyString endTag = PolyString.of( reader.getLocalName() );
                    if ( !tagStack.pop().equals( endTag ) ) {
                        throw new IllegalStateException( "Non-matching end tag: " + endTag );
                    }

                    PolyValue current = currentDoc;
                    if ( currentDoc.isEmpty() ) {
                        current = PolyNull.NULL;
                    } else if ( currentDoc.size() == 1 && currentDoc.containsKey( TEXT ) ) {
                        current = currentDoc.get( TEXT );
                    }
                    currentDoc = contextStack.pop();
                    putWithDuplicates( currentDoc, endTag, current );
                }
            }
        }
        if ( !contextStack.isEmpty() || !tagStack.isEmpty() ) {
            throw new IllegalStateException( "Stack is not empty, processing failed" );
        }
        return rootDoc;
    }


    private static void putWithDuplicates( PolyDocument doc, PolyString key, PolyValue value ) {
        if ( doc.containsKey( key ) ) {
            PolyValue existing = doc.get( key );
            if ( existing.isList() ) {
                existing.asList().add( value );
            } else {
                doc.put( key, PolyList.of( existing, value ) );
            }
        } else {
            doc.put( key, value ); // If not a duplicate, just add it to the doc
        }
    }


    private static PolyValue getCasted( String value, boolean cast ) {
        PolyString strValue = PolyString.of( value );
        if ( !cast ) {
            return strValue;
        }
        PolyType polyType = ActivityUtils.inferPolyType( value, PolyType.TEXT );
        return ActivityUtils.castPolyValue( strValue, polyType );

    }


    @Override
    public long estimateTupleCount( List<AlgDataType> inTypes, Settings settings, List<Long> inCounts, Supplier<Transaction> transactionSupplier ) {
        if ( settings.getString( "pointer" ).isBlank() ) {
            return 1; // only the root element
        }
        return 1_000_000; // an arbitrary estimate to show some progress for larger files
    }

}
