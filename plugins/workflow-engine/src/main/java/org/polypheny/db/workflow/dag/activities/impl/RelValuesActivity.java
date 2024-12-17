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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.relational.LogicalRelValues;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.Fusable;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.IntSetting;
import org.polypheny.db.workflow.dag.settings.BoolValue;
import org.polypheny.db.workflow.dag.settings.IntValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.writer.RelWriter;

@Slf4j

@ActivityDefinition(type = "relValues", displayName = "Constant Table", categories = { ActivityCategory.EXTRACT, ActivityCategory.RELATIONAL },
        inPorts = {},
        outPorts = { @OutPort(type = PortType.REL) }
)
@IntSetting(key = "rowCount", displayName = "Row Count", defaultValue = 3, min = 1, max = 1_000_000)
@BoolSetting(key = "fixSeed", displayName = "Fix Random Seed", defaultValue = false)

@SuppressWarnings("unused")
public class RelValuesActivity implements Activity, Fusable, Pipeable {

    public static final List<String> NAMES = List.of( "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Hank" );
    public static final List<String> LAST_NAMES = List.of( "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis" );


    @Override
    public List<Optional<AlgDataType>> previewOutTypes( List<Optional<AlgDataType>> inTypes, SettingsPreview settings ) throws ActivityException {
        return Activity.wrapType( getType() );
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        List<List<PolyValue>> values = getValues(
                settings.get( "rowCount", IntValue.class ).getValue(),
                settings.get( "fixSeed", BoolValue.class ).getValue()
        );
        RelWriter writer = ctx.createRelWriter( 0, getType(), true );
        writer.write( values.iterator() );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return getType();
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        int n = settings.get( "rowCount", IntValue.class ).getValue();
        boolean fixSeed = settings.get( "fixSeed", BoolValue.class ).getValue();

        Random random = fixSeed ? new Random( 42 ) : new Random();
        for ( int i = 0; i < n; i++ ) {
            List<PolyValue> tuple = getValue( random );
            output.put( tuple );
            log.info( "Value pipe inserted " + tuple );
        }
    }


    @Override
    public AlgNode fuse( List<AlgNode> inputs, Settings settings, AlgCluster cluster ) throws Exception {
        List<List<PolyValue>> values = getValues(
                settings.get( "rowCount", IntValue.class ).getValue(),
                settings.get( "fixSeed", BoolValue.class ).getValue()
        );
        return LogicalRelValues.create( cluster, getType(), toRexLiterals( getType(), values ) );
    }


    @Override
    public void reset() {

    }


    private static AlgDataType getType() {
        AlgDataTypeFactory typeFactory = AlgDataTypeFactory.DEFAULT;
        return typeFactory.builder()
                .add( null, StorageManager.PK_COL, null, typeFactory.createPolyType( PolyType.BIGINT ) )
                .add( null, "name", null, typeFactory.createPolyType( PolyType.VARCHAR, 50 ) )
                .add( null, "lastName", null, typeFactory.createPolyType( PolyType.VARCHAR, 50 ) )
                .add( null, "age", null, typeFactory.createPolyType( PolyType.INTEGER ) )
                .add( null, "salary", null, typeFactory.createPolyType( PolyType.INTEGER ) )
                .build();
    }


    private static List<List<PolyValue>> getValues( int n, boolean fixSeed ) {
        Random random = fixSeed ? new Random( 42 ) : new Random();
        List<List<PolyValue>> tuples = new ArrayList<>();
        for ( int i = 0; i < n; i++ ) {
            tuples.add( getValue( random ) );
        }
        return tuples;
    }


    private static List<PolyValue> getValue( Random random ) {
        String firstName = NAMES.get( random.nextInt( NAMES.size() ) );
        String lastName = LAST_NAMES.get( random.nextInt( LAST_NAMES.size() ) );
        int age = random.nextInt( 18, 66 );
        int salary = random.nextInt( 5000, 10000 );
        return getRow( firstName, lastName, age, salary );
    }


    private static ImmutableList<ImmutableList<RexLiteral>> toRexLiterals( AlgDataType tupleType, List<List<PolyValue>> rows ) {
        RexBuilder builder = new RexBuilder( AlgDataTypeFactory.DEFAULT );
        List<ImmutableList<RexLiteral>> records = new ArrayList<>();
        List<AlgDataType> fieldTypes = tupleType.getFields().stream().map( AlgDataTypeField::getType ).toList();
        for ( final List<PolyValue> row : rows ) {
            final List<RexLiteral> record = new ArrayList<>();
            for ( int i = 0; i < row.size(); ++i ) {
                // TODO: fix creation of RexLiteral
                PolyValue value = row.get( i );
                AlgDataType type = fieldTypes.get( i );
                record.add( new RexLiteral( value, type, type.getPolyType() ) );

                //record.add( builder.makeLiteral( value, type, type.getPolyType() ) );
            }
            records.add( ImmutableList.copyOf( record ) );
        }
        return ImmutableList.copyOf( records );
    }


    private static List<PolyValue> getRow( String name, String lastName, int age, int salary ) {
        return List.of(
                PolyLong.of( 0 ),
                PolyString.of( name ),
                PolyString.of( lastName ),
                PolyInteger.of( age ),
                PolyInteger.of( salary ) );
    }

}
