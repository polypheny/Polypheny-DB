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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.util.Pair;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.VariableWriter;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.FieldSelectSetting;
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.FieldSelectValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.dag.variables.WritableVariableStore;
import org.polypheny.db.workflow.engine.execution.Executor.ExecutorException;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContextImpl;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.reader.RelReader;

@ActivityDefinition(type = "relToVar", displayName = "Row to Variable", categories = { ActivityCategory.VARIABLES, ActivityCategory.RELATIONAL },
        inPorts = { @InPort(type = PortType.REL) },
        outPorts = {},
        shortDescription = "Transform one or multiple rows into variables."
)

@BoolSetting(key = "allRows", displayName = "Include All Rows", defaultValue = false, pos = 0,
        shortDescription = "If true, each column is mapped to an array containing the values of every row. Otherwise, only the first row is used.")
@BoolSetting(key = "singleVariable", displayName = "Map to Single Variable", defaultValue = false, pos = 1,
        shortDescription = "If true, the selected columns become fields of a single variable. Otherwise, each selected column becomes its own variable.")

@StringSetting(key = "varName", displayName = "Variable Name", minLength = 1, maxLength = 128, nonBlank = true, defaultValue = "row", pos = 2,
        shortDescription = "The name of the variable to be generated.", subPointer = "singleVariable", subValues = { "true" })

@StringSetting(key = "varPrefix", displayName = "Variable Prefix", minLength = 0, maxLength = 128, pos = 3,
        shortDescription = "An optional prefix to append to the column names.", subPointer = "singleVariable", subValues = { "false" })
@FieldSelectSetting(key = "columns", displayName = "Select Columns", reorder = false, pos = 4, defaultUnspecified = true,
        shortDescription = "Select the columns that get written to variables.", subPointer = "singleVariable", subValues = { "false" })

@SuppressWarnings("unused")
public class RelToVariableActivity implements VariableWriter {

    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        return List.of();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContextImpl ctx, WritableVariableStore writer ) throws ExecutorException {
        RelReader reader = (RelReader) inputs.get( 0 );
        boolean allRows = settings.getBool( "allRows" );

        if ( reader.getTupleCount() == 0 ) {
            ctx.throwException( "Input table must not be empty" );
        }

        List<String> colNames = reader.getTupleType().getFieldNames();
        Map<String, PolyValue> values = new HashMap<>();
        for ( List<PolyValue> row : reader.getIterable() ) {
            for ( Pair<String, PolyValue> pair : Pair.zip( colNames, row ) ) {
                if ( pair.left.equals( PK_COL ) ) {
                    continue;
                }
                if ( allRows ) {
                    PolyList<PolyValue> list = values.computeIfAbsent( pair.left, ( k ) -> PolyList.of() ).asList();
                    list.add( pair.right );
                } else {
                    values.put( pair.left, pair.right );
                }
            }
            if ( !allRows ) {
                break; // only first row is relevant
            }
        }

        if ( settings.getBool( "singleVariable" ) ) {
            String variableName = settings.getString( "varName" );
            Map<PolyString, PolyValue> docMap = values.entrySet().stream().collect(
                    Collectors.toMap( e -> PolyString.of( e.getKey() ), Entry::getValue ) );
            writer.setVariable( variableName, PolyDocument.ofDocument( docMap ) );
        } else {
            String prefix = settings.getString( "varPrefix" );
            FieldSelectValue select = settings.get( "columns", FieldSelectValue.class );
            for ( Entry<String, PolyValue> entry : values.entrySet() ) {
                String key = entry.getKey();
                if ( select == null || select.isSelected( key ) ) {
                    writer.setVariable( prefix + key, entry.getValue() );
                }
            }
        }
    }

}
