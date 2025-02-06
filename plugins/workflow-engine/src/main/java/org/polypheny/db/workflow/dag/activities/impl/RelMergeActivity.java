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
import java.util.Objects;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.RelType;
import org.polypheny.db.workflow.dag.activities.TypePreview.UnknownType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.writer.RelWriter;

@ActivityDefinition(type = "relMerge", displayName = "Relational Merge", categories = { ActivityCategory.TRANSFORM, ActivityCategory.RELATIONAL },
        inPorts = { @InPort(type = PortType.REL, isMulti = true, description = "One or more tables, some or all might not be active.") },
        outPorts = { @OutPort(type = PortType.REL, description = "The union of all active inputs.") },
        shortDescription = "Writes the rows of all active input tables into a single output table. "
                + "Unlike a Union activity, this activity produces a result even if some inputs are inactive. This is useful for merging conditional branches."
)

@SuppressWarnings("unused")
public class RelMergeActivity implements Activity {
    // Not Pipeable on purpose to ensure a failure on one input does not abort the other input


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        List<AlgDataType> presentTypes = inTypes.stream().filter( TypePreview::isPresent ).map( TypePreview::getNullableType ).toList();

        if ( presentTypes.isEmpty() ) {
            return UnknownType.ofRel().asOutTypes();
        }
        if ( presentTypes.size() == 1 ) {
            return RelType.of( presentTypes.get( 0 ) ).asOutTypes();
        }
        AlgDataType type = ActivityUtils.mergeTypesOrThrow( presentTypes );
        return RelType.of( type ).asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {

        List<AlgDataType> types = inputs.stream().filter( Objects::nonNull ).map( CheckpointReader::getTupleType ).toList();
        AlgDataType type = ActivityUtils.mergeTypesOrThrow( types );

        RelWriter writer = ctx.createRelWriter( 0, type );
        for ( CheckpointReader reader : inputs ) {
            if ( reader == null ) {
                continue;
            }
            writer.write( reader.getIterator(), ctx );
        }
    }


    @Override
    public DataStateMerger getDataStateMerger() {
        return DataStateMerger.OR;
    }

}
