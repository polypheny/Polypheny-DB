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
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.UnknownType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointQuery;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@ActivityDefinition(type = "relMerge", displayName = "Relational Merge", categories = { ActivityCategory.TRANSFORM, ActivityCategory.RELATIONAL },
        inPorts = { @InPort(type = PortType.REL), @InPort(type = PortType.REL, isOptional = true) }, // TODO: check if optional works
        outPorts = { @OutPort(type = PortType.REL) }
)

@SuppressWarnings("unused")
public class RelMergeActivity implements Activity {

    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        TypePreview first = inTypes.get( 0 ), second = inTypes.get( 1 );
        if ( first.isPresent() && second.isEmpty() ) {
            return first.asOutTypes();
        } else if ( second.isPresent() && first.isEmpty() ) {
            return second.asOutTypes();
        } else if ( first.isPresent() && second.isPresent() ) {
            AlgDataType type = ActivityUtils.mergeTypesOrThrow( List.of( first.getNullableType(), second.getNullableType() ) );
            return TypePreview.ofType( type ).asOutTypes();
        }
        return UnknownType.ofRel().asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        CheckpointReader input0 = inputs.get( 0 ), input1 = inputs.get( 1 );
        if ( input0 == null || input1 == null ) {
            assert (input0 != null || input1 != null);
            CheckpointReader input = input0 != null ? input0 : input1;

            ctx.createRelWriter( 0, input.getTupleType(), true )
                    .write( input.getIterator() );
            return;
        }

        CheckpointQuery query = CheckpointQuery.builder()
                .queryLanguage( "SQL" )
                .query( "SELECT * FROM " + CheckpointQuery.ENTITY( 0 ) + " UNION ALL SELECT * FROM " + CheckpointQuery.ENTITY( 1 ) )
                .build();
        Pair<AlgDataType, Iterator<List<PolyValue>>> result = input0.getIteratorFromQuery( query, inputs );
        ctx.createRelWriter( 0, result.left, true )
                .write( result.right );
    }


    @Override
    public void reset() {

    }


    @Override
    public DataStateMerger getDataStateMerger() {
        return DataStateMerger.OR;
    }

}
