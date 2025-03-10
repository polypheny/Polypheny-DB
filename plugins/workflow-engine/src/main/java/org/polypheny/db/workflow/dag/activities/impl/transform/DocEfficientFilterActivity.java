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

package org.polypheny.db.workflow.dag.activities.impl.transform;

import java.util.List;
import java.util.function.Predicate;
import org.polypheny.db.algebra.type.AlgDataType;
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
import org.polypheny.db.workflow.dag.annotations.FieldSelectSetting;
import org.polypheny.db.workflow.dag.annotations.FilterSetting;
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.FieldSelectValue;
import org.polypheny.db.workflow.dag.settings.FilterValue;
import org.polypheny.db.workflow.dag.settings.GroupDef;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue.SelectMode;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.dag.settings.StringSettingDef.AutoCompleteType;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@ActivityDefinition(type = "docEfficientFilter", displayName = "Filter Documents", categories = { ActivityCategory.TRANSFORM, ActivityCategory.DOCUMENT, ActivityCategory.CLEANING, ActivityCategory.ESSENTIALS },
        inPorts = { @InPort(type = PortType.DOC, description = "The input collection.") },
        outPorts = { @OutPort(type = PortType.DOC, description = "A collection containing all matching documents from the input collection.") },
        shortDescription = "Filters the documents of a collection based on a list of filter conditions."
)

@StringSetting(key = "pointer", displayName = "Relative Path", pos = 1, group = GroupDef.ADVANCED_GROUP, autoCompleteType = AutoCompleteType.FIELD_NAMES,
        shortDescription = "Optionally specify a (sub)field such as 'owner.address'. All conditions will act relative to this field. If a document does not contain that field, the filter matches by default.")
@BoolSetting(key = "all", displayName = "Match Subfields", pos = 2,
        subPointer = "filter/targetMode", subValues = { "\"REGEX\"" }, defaultValue = false,
        shortDescription = "If enabled, the search extends to all subfields.")
@FieldSelectSetting(key = "requiredFields", displayName = "Required Fields", simplified = true, targetInput = 0, pos = 3,
        shortDescription = "Specify all (sub)fields that must exist in the document. Useful in combination with a condition, as conditions evaluate to true if the target field does not exist.")
@FilterSetting(key = "filter", displayName = "Conditions", pos = 4,
        modes = { SelectMode.EXACT, SelectMode.REGEX },
        shortDescription = "Define a list of conditions on fields. In 'Exact' mode, subfields can be specified. If a (sub)field does not exist, the condition evaluates to true.")
@BoolSetting(key = "negate", displayName = "Negate Filter", pos = 5, defaultValue = false, group = GroupDef.ADVANCED_GROUP,
        shortDescription = "If enabled, the filter is negated.")

@SuppressWarnings("unused")
public class DocEfficientFilterActivity implements Activity, Pipeable {
    // Fusable is much harder to implement, but is theoretically possible


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        if ( inTypes.get( 0 ) instanceof DocType docType ) {
            return docType.asOutTypes(); // we assume that no top level fields get lost during filtering
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
        FilterValue filter = settings.get( "filter", FilterValue.class );
        String pointer = settings.getString( "pointer" );
        Predicate<PolyDocument> predicate = filter.getDocPredicate( settings.getBool( "all" ), settings.getString( "pointer" ).trim() );

        List<String> required = settings.get( "requiredFields", FieldSelectValue.class ).getInclude();
        if ( !required.isEmpty() ) {
            Predicate<PolyDocument> basePredicate = predicate;
            predicate = d -> {
                if ( required.stream().allMatch( f -> ActivityUtils.hasSubValue( d, f ) ) ) {
                    return basePredicate.test( d );
                }
                return false;
            };
        }
        if ( settings.getBool( "negate" ) ) {
            predicate = predicate.negate();
        }

        for ( List<PolyValue> row : inputs.get( 0 ) ) {
            PolyDocument doc = row.get( 0 ).asDocument();
            if ( predicate.test( doc ) ) {
                if ( !output.put( doc ) ) {
                    finish( inputs );
                    return;
                }
            }
        }
    }

}
