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

import java.util.List;
import java.util.function.Predicate;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.EnumSetting;
import org.polypheny.db.workflow.dag.annotations.FilterSetting;
import org.polypheny.db.workflow.dag.settings.FilterValue;
import org.polypheny.db.workflow.dag.settings.FilterValue.Operator;
import org.polypheny.db.workflow.dag.settings.GroupDef;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue.SelectMode;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.reader.RelReader;
import org.polypheny.db.workflow.engine.storage.writer.RelWriter;

@ActivityDefinition(type = "relFilter", displayName = "Filter Rows", categories = { ActivityCategory.TRANSFORM, ActivityCategory.RELATIONAL },
        inPorts = { @InPort(type = PortType.REL, description = "The input table.") },
        outPorts = {
                @OutPort(type = PortType.REL, description = "A table containing all matching rows from the input table."),
                @OutPort(type = PortType.REL, description = "A table containing all rows that did not meet the filter criteria.")
        },
        shortDescription = "Horizontally partitions the rows of a table based on a list of filter conditions."
)

@FilterSetting(key = "filter", displayName = "Filter Conditions", pos = 1,
        excludedOperators = { Operator.HAS_KEY, Operator.IS_OBJECT },
        modes = { SelectMode.EXACT, SelectMode.REGEX, SelectMode.INDEX })

@EnumSetting(
        key = "rejectedHandler", pos = 1, group = GroupDef.ADVANCED_GROUP,
        displayName = "Handling of Rejected Rows",
        options = { "store", "ignore", "fail" },
        defaultValue = "store",
        displayOptions = { "Keep", "Ignore", "Fail Execution" },
        displayDescriptions = { "Send the rejected rows to the second output.", "Skip rejected rows for better performance.", "A row that does not match the filter results in the activity to fail." },
        shortDescription = "Defines the behavior for rows that do not match the filter criteria."
)
@BoolSetting(key = "negate", displayName = "Negate Filter", pos = 2, defaultValue = false, group = GroupDef.ADVANCED_GROUP,
        shortDescription = "If enabled, the filter is negated, swapping the roles of the two outputs.")

@SuppressWarnings("unused")
public class RelFilterActivity implements Activity {


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        TypePreview outType = inTypes.get( 0 ).asOutType();
        return List.of( outType, outType );
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        RelReader reader = (RelReader) inputs.get( 0 );
        RelWriter matchWriter = ctx.createRelWriter( 0, reader.getTupleType() );
        RelWriter rejectWriter = ctx.createRelWriter( 1, reader.getTupleType() );

        String rejectedHandler = settings.getString( "rejectedHandler" );
        FilterValue filter = settings.get( "filter", FilterValue.class );
        Predicate<List<PolyValue>> predicate = filter.getRelPredicate( reader.getTupleType().getFieldNames() );
        if ( settings.getBool( "negate" ) ) {
            predicate = predicate.negate();
        }

        long inCount = reader.getRowCount();
        long countDelta = Math.max( inCount / 100, 1 );
        long count = 0;
        for ( List<PolyValue> row : reader.getIterable() ) {
            if ( predicate.test( row ) ) {
                matchWriter.write( row );
            } else {
                switch ( rejectedHandler ) {
                    case "store" -> rejectWriter.write( row );
                    case "fail" -> throw new GenericRuntimeException( "Detected row that does not match the filter criteria: " + row );
                }
            }

            count++;
            if ( count % countDelta == 0 ) {
                ctx.updateProgress( (double) count / inCount );
            }
            ctx.checkInterrupted();
        }
    }

}
