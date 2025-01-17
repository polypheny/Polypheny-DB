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

import static org.polypheny.db.workflow.dag.activities.impl.RelValuesActivity.LAST_NAMES;
import static org.polypheny.db.workflow.dag.activities.impl.RelValuesActivity.NAMES;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.document.LogicalDocumentValues;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.Fusable;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.DocType;
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
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.writer.DocWriter;

@Slf4j

@ActivityDefinition(type = "docValues", displayName = "Constant Collection", categories = { ActivityCategory.EXTRACT, ActivityCategory.DOCUMENT },
        inPorts = {},
        outPorts = { @OutPort(type = PortType.DOC) }
)
@IntSetting(key = "count", displayName = "Document Count", defaultValue = 3, min = 1, max = 1_000_000)
@BoolSetting(key = "fixSeed", displayName = "Fix Random Seed", defaultValue = false)

@SuppressWarnings("unused")
public class DocValuesActivity implements Activity, Fusable, Pipeable {


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        return DocType.of().asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        DocWriter writer = ctx.createDocWriter( 0 );
        writer.writeFromIterator( getValues(
                settings.get( "count", IntValue.class ).getValue(),
                settings.get( "fixSeed", BoolValue.class ).getValue()
        ).iterator() );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return getDocType();
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        int n = settings.get( "count", IntValue.class ).getValue();
        boolean fixSeed = settings.get( "fixSeed", BoolValue.class ).getValue();

        Random random = fixSeed ? new Random( 42 ) : new Random();
        for ( int i = 0; i < n; i++ ) {
            PolyDocument doc = getValue( random );
            output.put( doc );
        }
    }


    @Override
    public long estimateTupleCount( List<AlgDataType> inTypes, Settings settings, List<Long> inCounts, Supplier<Transaction> transactionSupplier ) {
        return settings.get( "count", IntValue.class ).getValue();
    }


    @Override
    public AlgNode fuse( List<AlgNode> inputs, Settings settings, AlgCluster cluster ) throws Exception {
        List<PolyDocument> values = getValues(
                settings.get( "count", IntValue.class ).getValue(),
                settings.get( "fixSeed", BoolValue.class ).getValue()
        );
        return LogicalDocumentValues.create( cluster, values );
    }


    private static List<PolyDocument> getValues( int n, boolean fixSeed ) {
        Random random = fixSeed ? new Random( 42 ) : new Random();
        List<PolyDocument> documents = new ArrayList<>();
        for ( int i = 0; i < n; i++ ) {
            documents.add( getValue( random ) );
        }
        return documents;
    }


    private static PolyDocument getValue( Random random ) {
        String firstName = NAMES.get( random.nextInt( NAMES.size() ) );
        String lastName = LAST_NAMES.get( random.nextInt( LAST_NAMES.size() ) );
        int age = random.nextInt( 18, 66 );
        int salary = random.nextInt( 5000, 10000 );
        return getDocument( firstName, lastName, age, salary );
    }


    private static PolyDocument getDocument( String name, String lastName, int age, int salary ) {
        Map<PolyString, PolyValue> map = new HashMap<>();
        map.put( PolyString.of( "name" ), PolyString.of( name ) );
        map.put( PolyString.of( "lastName" ), PolyString.of( lastName ) );
        map.put( PolyString.of( "age" ), PolyInteger.of( age ) );
        map.put( PolyString.of( "salary" ), PolyInteger.of( salary ) );
        return new PolyDocument( map );
    }

}
