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
import java.util.Optional;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidInputException;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointQuery;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.writer.CheckpointWriter;

@ActivityDefinition(type = "relUnion", displayName = "Relational Union", categories = { ActivityCategory.TRANSFORM, ActivityCategory.RELATIONAL },
        inPorts = { @InPort(type = PortType.REL), @InPort(type = PortType.REL) },
        outPorts = { @OutPort(type = PortType.REL) }
)
public class RelUnionActivity implements Activity {

    @Override
    public List<Optional<AlgDataType>> previewOutTypes( List<Optional<AlgDataType>> inTypes, SettingsPreview settings ) throws ActivityException {

        Optional<AlgDataType> firstType = inTypes.get( 0 );
        Optional<AlgDataType> secondType = inTypes.get( 1 );
        if ( firstType.isEmpty() || secondType.isEmpty() ) {
            return List.of( Optional.empty() );
        }
        if ( !firstType.get().equals( secondType.get() ) ) {
            throw new InvalidInputException( "The second input type is not equal to the first input type", 1 );
        }
        return List.of( firstType );
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        CheckpointQuery query = CheckpointQuery.builder()
                .queryLanguage( "SQL" )
                .query( "SELECT * FROM " + CheckpointQuery.ENTITY( 0 ) + " UNION ALL SELECT * FROM " + CheckpointQuery.ENTITY( 1 ) )
                .build();
        try ( CheckpointWriter writer = ctx.createRelWriter( 0, inputs.get( 0 ).getTupleType(), true ) ) {
            writer.write( inputs.get( 0 ).getIteratorFromQuery( query, inputs ) );
        }

    }


    @Override
    public void reset() {

    }

}
