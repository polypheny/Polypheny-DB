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

package org.polypheny.db.workflow.dag.activities.impl.extract;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.relational.LogicalRelValues;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.transaction.Transaction;
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
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.IntSetting;
import org.polypheny.db.workflow.dag.settings.BoolValue;
import org.polypheny.db.workflow.dag.settings.IntValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.FuseExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@Slf4j

@ActivityDefinition(type = "relValues", displayName = "Generate Random Table", categories = { ActivityCategory.EXTRACT, ActivityCategory.RELATIONAL, ActivityCategory.DEVELOPMENT },
        inPorts = {},
        outPorts = { @OutPort(type = PortType.REL) },
        shortDescription = "Generates a random table emulating employee data."
)
@IntSetting(key = "rowCount", displayName = "Row Count", defaultValue = 3, min = 1, max = 10_000_000,
        shortDescription = "The number of rows to generate.")
@BoolSetting(key = "fixSeed", displayName = "Fix Random Seed", defaultValue = false,
        shortDescription = "If enabled, ensures the same random values are generated each time.")

@SuppressWarnings("unused")
public class RelValuesActivity implements Activity, Fusable, Pipeable {

    public static final List<String> NAMES = List.of( "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Hank" );
    public static final List<String> LAST_NAMES = List.of( "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis" );


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        return TypePreview.ofType( getType() ).asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        Pipeable.super.execute( inputs, settings, ctx );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return getType();
    }


    @Override
    public long estimateTupleCount( List<AlgDataType> inTypes, Settings settings, List<Long> inCounts, Supplier<Transaction> transactionSupplier ) {
        return settings.get( "rowCount", IntValue.class ).getValue();
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        int n = settings.get( "rowCount", IntValue.class ).getValue();
        boolean fixSeed = settings.get( "fixSeed", BoolValue.class ).getValue();

        Random random = fixSeed ? new Random( 42 ) : new Random();
        for ( int i = 0; i < n; i++ ) {
            List<PolyValue> tuple = getValue( random );
            if ( !output.put( tuple ) ) {
                break;
            }
        }
    }


    @Override
    public AlgNode fuse( List<AlgNode> inputs, Settings settings, AlgCluster cluster, FuseExecutionContext ctx ) throws Exception {
        List<List<PolyValue>> values = getValues(
                settings.get( "rowCount", IntValue.class ).getValue(),
                settings.get( "fixSeed", BoolValue.class ).getValue()
        );
        return LogicalRelValues.create( cluster, getType(), toRexLiterals( getType(), values ) );
    }


    @Override
    public Optional<Boolean> canFuse( List<TypePreview> inTypes, SettingsPreview settings ) {
        return settings.get( "rowCount", IntValue.class ).map( v -> v.getValue() <= 250 ); // otherwise the amount of generated code grows too big
    }


    private static AlgDataType getType() {
        return factory.builder()
                .add( null, StorageManager.PK_COL, null, factory.createPolyType( PolyType.BIGINT ) )
                .add( null, "name", null, factory.createPolyType( PolyType.VARCHAR, 50 ) ).nullable( true )
                .add( null, "lastname", null, factory.createPolyType( PolyType.VARCHAR, 50 ) ).nullable( true )
                .add( null, "age", null, factory.createPolyType( PolyType.INTEGER ) ).nullable( true )
                .add( null, "salary", null, factory.createPolyType( PolyType.INTEGER ) ).nullable( true )
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
        List<ImmutableList<RexLiteral>> records = new ArrayList<>();
        List<AlgDataType> fieldTypes = tupleType.getFields().stream().map( AlgDataTypeField::getType ).toList();
        for ( final List<PolyValue> row : rows ) {
            final List<RexLiteral> record = new ArrayList<>();
            for ( int i = 0; i < row.size(); ++i ) {
                PolyValue value = row.get( i );
                AlgDataType type = fieldTypes.get( i );
                record.add( new RexLiteral( value, type, type.getPolyType() ) );
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
