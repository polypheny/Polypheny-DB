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

package org.polypheny.db.workflow.dag.activities.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.DocType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.DefaultGroup;
import org.polypheny.db.workflow.dag.annotations.Group.Subgroup;
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.dag.settings.StringSettingDef.AutoCompleteType;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@ActivityDefinition(type = "docInsert", displayName = "Document Insert", categories = { ActivityCategory.TRANSFORM, ActivityCategory.DOCUMENT },
        inPorts = {
                @InPort(type = PortType.DOC, description = "The target collection where the values are inserted."),
                @InPort(type = PortType.DOC, description = "The source collection containing the value(s) to insert.")
        },
        outPorts = { @OutPort(type = PortType.DOC, description = "The modified target collection.") },
        shortDescription = "Inserts the specified value from document(s) in the source collection into the target collection."
)

@StringSetting(key = "target", displayName = "Target Field", pos = 0,
        autoCompleteType = AutoCompleteType.FIELD_NAMES, autoCompleteInput = 0, maxLength = 1024, nonBlank = true,
        shortDescription = "Specify the target (sub)field in the first input (e.g. 'owner.address').")

@DefaultGroup(subgroups = { @Subgroup(key = "source", displayName = "Value to Insert") })
@StringSetting(key = "source", displayName = "Source Field", subGroup = "source", pos = 1,
        autoCompleteType = AutoCompleteType.FIELD_NAMES, autoCompleteInput = 1, maxLength = 1024,
        shortDescription = "Specify the source (sub)field in the second input or leave it empty to insert the entire document.")
@BoolSetting(key = "includeId", displayName = "Include ID", subGroup = "source", pos = 2,
        subPointer = "source", subValues = { "\"\"" }, defaultValue = false,
        shortDescription = "Whether to include the document-ID field when inserting entire documents.")
@BoolSetting(key = "all", displayName = "Insert All Source Documents", subGroup = "source", pos = 3,
        shortDescription = "If false, only the value of the first source document is inserted. If true and the source collection contains multiple documents, they are inserted as an array.")

@SuppressWarnings("unused")
public class DocInsertActivity implements Activity, Pipeable {

    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        Set<String> fields = new HashSet<>();
        String target = settings.getNullableString( "target" );
        if ( inTypes.get( 0 ) instanceof DocType type ) {
            fields.addAll( type.getKnownFields() );
        }
        if ( target != null ) {
            fields.add( target.split( "\\.", 2 )[0] );
        }
        return DocType.of( fields ).asOutTypes();
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
        InputPipe inTarget = inputs.get( 0 );
        InputPipe inSource = inputs.get( 1 );
        String target = settings.getString( "target" );
        PolyValue value = getValueToInsert( inSource, settings.getString( "source" ), settings.getBool( "all" ), settings.getBool( "includeId" ) );

        for ( List<PolyValue> tuple : inTarget ) {
            PolyDocument doc = tuple.get( 0 ).asDocument();
            ActivityUtils.insertSubValue( doc, target, value );
            if ( !output.put( doc ) ) {
                finish( inputs );
                return;
            }
        }
    }


    private PolyValue getValueToInsert( InputPipe inSource, String source, boolean all, boolean includeId ) throws Exception {
        List<PolyValue> values = new ArrayList<>();
        for ( List<PolyValue> tuple : inSource ) {
            PolyValue value = ActivityUtils.getSubValue( tuple.get( 0 ).asDocument(), source );
            if ( source.isBlank() && !includeId ) {
                value.asDocument().remove( Activity.docId );
            }
            values.add( value );
            if ( !all ) {
                inSource.finishIteration();
                break;
            }
        }

        if ( values.isEmpty() ) {
            throw new GenericRuntimeException( "Source collection must not be empty" );
        }
        if ( values.size() == 1 ) {
            return values.get( 0 );
        }
        return PolyList.of( values );
    }

}
