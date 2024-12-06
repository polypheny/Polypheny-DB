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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.writer.RelWriter;

@ActivityDefinition(type = "relValues", displayName = "Constant Table", categories = { ActivityCategory.EXTRACT },
        inPorts = {},
        outPorts = { @OutPort(type = PortType.REL) }
)
public class RelValuesActivity implements Activity {

    @Override
    public List<Optional<AlgDataType>> previewOutTypes( List<Optional<AlgDataType>> inTypes, Map<String, Optional<SettingValue>> settings ) throws ActivityException {
        return Activity.wrapType( getType() );
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Map<String, SettingValue> settings, ExecutionContext ctx ) throws Exception {
        try ( RelWriter writer = ctx.createRelWriter( 0, getType(), true ) ) {
            writer.write( getValues().iterator() );
        }
    }


    @Override
    public void reset() {

    }


    private static AlgDataType getType() {
        AlgDataTypeFactory typeFactory = AlgDataTypeFactory.DEFAULT;
        return typeFactory.builder()
                .add( null, StorageManager.PK_COL, null, typeFactory.createPolyType( PolyType.BIGINT ) )
                .add( null, "name", null, typeFactory.createPolyType( PolyType.VARCHAR, 50 ) )
                .add( null, "age", null, typeFactory.createPolyType( PolyType.INTEGER ) )
                .add( null, "gender", null, typeFactory.createPolyType( PolyType.VARCHAR, 1 ) )
                .build();
    }


    private static List<List<PolyValue>> getValues() {
        List<List<PolyValue>> tuples = new ArrayList<>();
        tuples.add( getRow( "Alice", 25, true ) );
        tuples.add( getRow( "Bob", 30, false ) );
        tuples.add( getRow( "Charlie", 35, false ) );
        return tuples;
    }


    private static List<PolyValue> getRow( String name, int age, boolean isFemale ) {
        return List.of( PolyInteger.of( 0 ), PolyString.of( name ), PolyInteger.of( age ), PolyString.of( isFemale ? "F" : "M" ) );
    }

}
