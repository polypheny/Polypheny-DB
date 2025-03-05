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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
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
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;


@ActivityDefinition(type = "docCreate", displayName = "Create Collection", categories = { ActivityCategory.EXTRACT, ActivityCategory.DOCUMENT, ActivityCategory.ESSENTIALS, ActivityCategory.DEVELOPMENT },
        inPorts = {},
        outPorts = { @OutPort(type = PortType.DOC) },
        shortDescription = "This activity can be used to create a collection from a given JSON string. It should only be used for small collections, as the data is stored in the workflow itself."
)
@StringSetting(key = "json", displayName = "JSON", textEditor = true, language = "json", nonBlank = true, maxLength = 60 * 1024, // TODO: limit size according to websocket message size limit (~64 KB)
        shortDescription = "A single document as a JSON object or a collection of documents as a JSON array of objects.")

@SuppressWarnings("unused")
public class DocCreateActivity implements Activity, Pipeable {

    private static final ObjectMapper mapper = new ObjectMapper();
    private List<ObjectNode> docObjects;


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        String str = settings.getNullableString( "json" );
        if ( str != null && str.length() < 50_000 ) { // only preview small objects
            try {
                JsonNode root = mapper.readTree( str );
                JsonNode firstObject;
                if ( root.isArray() && !root.isEmpty() ) {
                    firstObject = root.get( 0 );
                } else if ( root.isObject() ) {
                    firstObject = root;
                } else {
                    throw new InvalidSettingException( "The root must be an object or a non-empty array", "json" );
                }
                if ( !firstObject.isObject() ) {
                    throw new InvalidSettingException( "Not an object: " + firstObject, "json" );
                }
                Set<String> fields = new TreeSet<>(); // ordered keys
                firstObject.fieldNames().forEachRemaining( fields::add );
                return DocType.of( fields ).asOutTypes();

            } catch ( JsonProcessingException e ) {
                throw new InvalidSettingException( "Invalid JSON: " + e.getMessage(), "json" );
            }

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
        setDocObjects( settings.getString( "json" ) );
        for ( ObjectNode node : docObjects ) {
            PolyDocument doc = getDocument( node );
            if ( !output.put( doc ) ) {
                break;
            }
        }
    }


    @Override
    public long estimateTupleCount( List<AlgDataType> inTypes, Settings settings, List<Long> inCounts, Supplier<Transaction> transactionSupplier ) {
        try {
            setDocObjects( settings.getString( "json" ) );
        } catch ( Exception e ) {
            docObjects = null;
            return -1;
        }
        return docObjects.size();
    }


    @Override
    public void reset() {
        docObjects = null;
    }


    public static PolyDocument getDocument( ObjectNode node ) {
        PolyValue value = PolyValue.fromJson( node.toString() );
        if ( value == null || !value.isDocument() ) {
            throw new GenericRuntimeException( "Cannot create document from object: " + node + ", " + value );
        }
        PolyDocument doc = value.asDocument();
        ActivityUtils.addDocId( doc );
        return doc;
    }


    private void setDocObjects( String json ) throws Exception {
        if ( docObjects != null ) {
            return;
        }
        docObjects = new ArrayList<>();

        JsonNode root = mapper.readTree( json );
        if ( root.isArray() && !root.isEmpty() ) {
            for ( JsonNode node : root ) {
                if ( !node.isObject() ) {
                    throw new InvalidSettingException( "Not an object: " + node, "json" );
                }
                docObjects.add( (ObjectNode) node );
            }
        } else if ( root.isObject() ) {
            docObjects.add( (ObjectNode) root );
        } else {
            throw new InvalidSettingException( "The root must be an object or a non-empty array", "json" );
        }

    }

}
