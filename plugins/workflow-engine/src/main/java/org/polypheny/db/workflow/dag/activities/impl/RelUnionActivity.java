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

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.relational.LogicalRelUnion;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidInputException;
import org.polypheny.db.workflow.dag.activities.Fusable;
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
public class RelUnionActivity implements Activity, Fusable {

    @Override
    public List<Optional<AlgDataType>> previewOutTypes( List<Optional<AlgDataType>> inTypes, SettingsPreview settings ) throws ActivityException {

        Optional<AlgDataType> firstType = inTypes.get( 0 );
        Optional<AlgDataType> secondType = inTypes.get( 1 );
        if ( firstType.isEmpty() || secondType.isEmpty() ) {
            return List.of( Optional.empty() );
        }
        return Activity.wrapType( getTypeOrThrow( List.of( firstType.get(), secondType.get() ) ) );
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        CheckpointQuery query = CheckpointQuery.builder()
                .queryLanguage( "SQL" )
                .query( "SELECT * FROM " + CheckpointQuery.ENTITY( 0 ) + " UNION ALL SELECT * FROM " + CheckpointQuery.ENTITY( 1 ) )
                .build();
        Pair<AlgDataType, Iterator<List<PolyValue>>> result = inputs.get( 0 ).getIteratorFromQuery( query, inputs );
        try ( CheckpointWriter writer = ctx.createRelWriter( 0, result.left, true ) ) {
            writer.write( result.right );
        }

    }


    @Override
    public AlgNode fuse( List<AlgNode> inputs, Settings settings, AlgCluster cluster ) throws Exception {
        System.out.println( "in types: " + inputs.stream().map( AlgNode::getTupleType ).toList() );
        return LogicalRelUnion.create( inputs, true );
    }


    @Override
    public void reset() {

    }


    public static AlgDataType getTypeOrThrow( List<AlgDataType> inputs ) throws InvalidInputException {
        AlgDataType type = AlgDataTypeFactory.DEFAULT.leastRestrictive( inputs );

        if ( type == null ) {
            throw new InvalidInputException( "The tuple types of the inputs are incompatible", 1 );
        }
        return type;
    }

}
