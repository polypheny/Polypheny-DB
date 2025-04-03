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

package org.polypheny.db.workflow.dag.activities.impl.variables;

import static org.polypheny.db.workflow.dag.variables.VariableStore.WORKFLOW_KEY;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.function.Predicate;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.FieldSelectSetting;
import org.polypheny.db.workflow.dag.annotations.FilterSetting;
import org.polypheny.db.workflow.dag.settings.FieldSelectValue;
import org.polypheny.db.workflow.dag.settings.FilterValue;
import org.polypheny.db.workflow.dag.settings.GroupDef;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue.SelectMode;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.dag.variables.ReadableVariableStore;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@ActivityDefinition(type = "variableValidate", displayName = "Validate Variable Values", categories = { ActivityCategory.VARIABLES },
        inPorts = {},
        outPorts = {},
        shortDescription = "Used to check whether given variable(s) meet the specified condition(s). If not, the activity fails. This becomes useful in combination with control edges."
)

@BoolSetting(key = "all", displayName = "Match Sub-Values", pos = 2,
        subPointer = "filter/targetMode", subValues = { "\"REGEX\"" }, defaultValue = false,
        shortDescription = "If enabled, the regex search extends to all variable sub-values.")
@FieldSelectSetting(key = "requiredVariables", displayName = "Required Variables", simplified = true, targetInput = -1, pos = 3,
        shortDescription = "Specify all variables or sub-values using dot-notation (e.g. \" + WORKFLOW_KEY + \".myVariable) that must be defined. Useful in combination with a condition, as conditions evaluate to true if the target variable does not exist.")
@FilterSetting(key = "filter", displayName = "Conditions", pos = 4,
        modes = { SelectMode.EXACT, SelectMode.REGEX }, targetInput = -1,
        shortDescription = "Define a list of conditions on variables or sub-values using dot-notation (e.g. " + WORKFLOW_KEY + ".myVariable). In 'Exact' mode, sub-values can be specified. If a (sub-)value does not exist, the condition evaluates to true.")
@BoolSetting(key = "negate", displayName = "Negate Validation", pos = 5, defaultValue = false, group = GroupDef.ADVANCED_GROUP,
        shortDescription = "If enabled, the validation condition is negated.")

@SuppressWarnings("unused")
public class VariableValidateActivity implements Activity {
    private static final ObjectMapper mapper = new ObjectMapper();


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        return List.of();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        FilterValue filter = settings.get( "filter", FilterValue.class );
        Predicate<PolyDocument> predicate = filter.getDocPredicate( settings.getBool( "all" ), "" );

        List<String> required = settings.get( "requiredVariables", FieldSelectValue.class ).getInclude();
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

        ReadableVariableStore variables = ctx.getVariableStore();

        String json = mapper.writeValueAsString( variables.getPublicVariables(true, false) );
        PolyDocument varDoc = PolyValue.fromJson( json ).asDocument();
        System.out.println(varDoc.toJson());

        if ( !predicate.test( varDoc ) ) {
            ctx.throwException( "A variable does not meet the specified condition." );
        }
    }

}
