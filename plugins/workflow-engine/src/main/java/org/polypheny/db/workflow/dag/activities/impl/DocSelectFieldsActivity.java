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

package org.polypheny.db.workflow.dag.activities.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.document.LogicalDocumentProject;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.rex.RexNameRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.Fusable;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.DocType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.EnumSetting;
import org.polypheny.db.workflow.dag.annotations.FieldSelectSetting;
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.FieldSelectValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.dag.settings.StringValue;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.FuseExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@ActivityDefinition(type = "docSelectFields", displayName = "Select Document Fields", categories = { ActivityCategory.TRANSFORM, ActivityCategory.DOCUMENT, },
        inPorts = { @InPort(type = PortType.DOC, description = "The input collection.") },
        outPorts = { @OutPort(type = PortType.DOC, description = "Collection containing only the specified fields of the input table. The '_id' field is always included.") },
        shortDescription = "Select a subset of (nested) fields of the input collection."
)
@EnumSetting(key = "mode", displayName = "Selection Mode", options = { "fieldSelect", "regex" }, displayOptions = { "Include / Exclude", "Regex" }, defaultValue = "fieldSelect", pos = 0)

@FieldSelectSetting(key = "fields", displayName = "Select Fields", reorder = false,
        subPointer = "mode", subValues = { "\"fieldSelect\"" },
        shortDescription = "Specify which fields to include. Alternatively, you can include any field not present in the list of excluded fields. Subfields can be specified using 'field.subfield'.")
@StringSetting(key = "regex", displayName = "Regex", maxLength = 1024, containsRegex = true,
        subPointer = "mode", subValues = { "\"regex\"" },
        shortDescription = "Specify the fields to include using a regular expression. This selection mode is less efficient as 'Include / Exclude' and does not allow for the selection of subfields.")

@SuppressWarnings("unused")
public class DocSelectFieldsActivity implements Activity, Fusable, Pipeable {

    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        if ( inTypes.get( 0 ) instanceof DocType inType ) {
            Set<String> fields = new HashSet<>( inType.getKnownFields() );
            String mode = settings.getNullableString( "mode" );
            if ( mode != null ) {
                switch ( mode ) {
                    case "fieldSelect" -> {
                        Optional<FieldSelectValue> select = settings.get( "fields", FieldSelectValue.class );
                        select.ifPresent( s -> {
                            s.getExclude().forEach( fields::remove );
                            if ( !s.includeUnspecified() ) {
                                fields.retainAll( s.getInclude() );
                            }
                        } );
                    }
                    case "regex" -> {
                        Optional<StringValue> regex = settings.get( "regex", StringValue.class );
                        regex.ifPresent( r -> {
                            List<String> matches = ActivityUtils.getRegexMatches( r.getValue(), fields.stream().toList() );
                            fields.retainAll( matches );
                        } );
                    }
                    default -> {
                    } // ignored
                }
            }
            return DocType.of( fields ).asOutTypes();
        }
        return DocType.of().asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        if (settings.getString( "mode" ).equals( "fieldSelect" )) {
            Fusable.super.execute( inputs, settings, ctx );
        } else {
            Pipeable.super.execute( inputs, settings, ctx );
        }
    }


    @Override
    public Optional<Boolean> canFuse( List<TypePreview> inTypes, SettingsPreview settings ) {
        String mode = settings.getString( "mode" );
        return Optional.of( mode.equals( "fieldSelect" ) );
    }


    @Override
    public AlgNode fuse( List<AlgNode> inputs, Settings settings, AlgCluster cluster, FuseExecutionContext ctx ) throws Exception {
        AlgDataType type = getDocType();
        FieldSelectValue setting = settings.get( "fields", FieldSelectValue.class );
        if ( setting.includeUnspecified() ) {
            return LogicalDocumentProject.create( inputs.get( 0 ), Map.of(), setting.getExclude() );
        }

        List<String> fields = setting.getInclude();
        Map<String, RexNode> nameRefs = fields.stream().collect( Collectors.toMap(
                field -> field,
                field -> RexNameRef.create( List.of( field.split( "\\." ) ), null, type )
        ) );
        return LogicalDocumentProject.create( inputs.get( 0 ), nameRefs, setting.getExclude() );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return getDocType();
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        assert settings.getString( "mode" ).equals( "regex" ) : "Pipe is currently only implemented for regex";
        Pattern pattern = Pattern.compile(settings.getString( "regex" ));

        for (List<PolyValue> tuple : inputs.get( 0 )) {
            PolyDocument doc = tuple.get( 0 ).asDocument();
            Map<PolyString, PolyValue> map = new HashMap<>();
            for ( Entry<PolyString, PolyValue> entry : doc.entrySet() ) {
                String key = entry.getKey().value;
                if (pattern.matcher( key ).matches() ) {
                    map.put( entry.getKey(), entry.getValue() );
                }
            }
            if (!output.put( PolyDocument.ofDocument( map ) )) {
                inputs.get( 0 ).finishIteration();
                return;
            }
        }
    }


    @Override
    public Optional<Boolean> canPipe( List<TypePreview> inTypes, SettingsPreview settings ) {
        String mode = settings.getString( "mode" );
        return Optional.of( mode.equals( "regex" ) );
    }

}
