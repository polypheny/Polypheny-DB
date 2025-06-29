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

import static org.polypheny.db.workflow.dag.activities.impl.extract.RelValuesActivity.LAST_NAMES;
import static org.polypheny.db.workflow.dag.activities.impl.extract.RelValuesActivity.NAMES;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.document.LogicalDocumentValues;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.util.BsonUtil;
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
import org.polypheny.db.workflow.dag.settings.IntValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.FuseExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.writer.DocWriter;

@Slf4j

@ActivityDefinition(type = "docValues", displayName = "Generate Random Collection", categories = { ActivityCategory.EXTRACT, ActivityCategory.DOCUMENT, ActivityCategory.DEVELOPMENT },
        inPorts = {},
        outPorts = { @OutPort(type = PortType.DOC) },
        shortDescription = "Generates a random collection emulating employee data."
)
@IntSetting(key = "count", displayName = "Document Count", defaultValue = 3, min = 1, max = 1_000_000,
        shortDescription = "The number of documents to generate.")
@BoolSetting(key = "fixSeed", displayName = "Fix Random Seed", defaultValue = false,
        shortDescription = "If enabled, ensures the same random values are generated each time.")
@BoolSetting(key = "array", displayName = "Include Array Data", defaultValue = true,
        shortDescription = "If enabled, each document has a + 'skills' array.")

@SuppressWarnings("unused")
public class DocValuesActivity implements Activity, Fusable, Pipeable {

    public static final List<String> SKILLS = List.of( "Java", "JavaScript", "Angular", "Git", "C++", "Python" );


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        Set<String> fields = new HashSet<>( Set.of( "name", "lastName", "age", "salary" ) );
        if ( settings.keysPresent( "array" ) && settings.getBool( "array" ) ) {
            fields.add( "skills" );
        }
        return DocType.of( fields ).asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        DocWriter writer = ctx.createDocWriter( 0 );
        writer.writeFromIterator( getValues(
                settings.getInt( "count" ),
                settings.getBool( "fixSeed" ),
                settings.getBool( "array" )
        ).iterator() );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return getDocType();
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        int n = settings.getInt( "count" );
        boolean fixSeed = settings.getBool( "fixSeed" );

        List<String> shuffled = settings.getBool( "array" ) ? new ArrayList<>( SKILLS ) : null;
        Random random = fixSeed ? new Random( 42 ) : new Random();
        for ( int i = 0; i < n; i++ ) {
            PolyDocument doc = getValue( random, shuffled );
            if ( !output.put( doc ) ) {
                break;
            }
        }
    }


    @Override
    public long estimateTupleCount( List<AlgDataType> inTypes, Settings settings, List<Long> inCounts, Supplier<Transaction> transactionSupplier ) {
        return settings.get( "count", IntValue.class ).getValue();
    }


    @Override
    public AlgNode fuse( List<AlgNode> inputs, Settings settings, AlgCluster cluster, FuseExecutionContext ctx ) throws Exception {
        List<PolyDocument> values = getValues(
                settings.getInt( "count" ),
                settings.getBool( "fixSeed" ),
                settings.getBool( "array" )
        );
        return new LogicalDocumentValues( cluster, cluster.traitSetOf( ModelTrait.DOCUMENT ), values );
    }


    @Override
    public Optional<Boolean> canFuse( List<TypePreview> inTypes, SettingsPreview settings ) {
        return settings.get( "count", IntValue.class ).map( v -> v.getValue() <= 250 ); // otherwise the amount of generated code grows too big
    }


    private static List<PolyDocument> getValues( int n, boolean fixSeed, boolean includeArray ) {
        Random random = fixSeed ? new Random( 42 ) : new Random();
        List<String> shuffled = includeArray ? new ArrayList<>( SKILLS ) : null;
        List<PolyDocument> documents = new ArrayList<>();
        for ( int i = 0; i < n; i++ ) {
            documents.add( getValue( random, shuffled ) );
        }
        return documents;
    }


    private static PolyDocument getValue( Random random, List<String> skills ) {
        String firstName = NAMES.get( random.nextInt( NAMES.size() ) );
        String lastName = LAST_NAMES.get( random.nextInt( LAST_NAMES.size() ) );
        int age = random.nextInt( 18, 66 );
        int salary = random.nextInt( 5000, 10000 );
        List<String> subSkills = null;
        if ( skills != null ) {
            Collections.shuffle( skills, random );
            subSkills = skills.subList( 0, random.nextInt( 0, skills.size() ) );
        }
        return getDocument( firstName, lastName, age, salary, subSkills );
    }


    private static PolyDocument getDocument( String name, String lastName, int age, int salary, List<String> skills ) {
        Map<PolyString, PolyValue> map = new HashMap<>();
        map.put( docId, PolyString.of( BsonUtil.getObjectId() ) );
        map.put( PolyString.of( "name" ), PolyString.of( name ) );
        map.put( PolyString.of( "lastName" ), PolyString.of( lastName ) );
        map.put( PolyString.of( "age" ), PolyInteger.of( age ) );
        map.put( PolyString.of( "salary" ), PolyInteger.of( salary ) );
        if ( skills != null ) {
            map.put( PolyString.of( "skills" ), getSkillsArray( skills ) );
        }

        return new PolyDocument( map );
    }


    private static PolyList<PolyDocument> getSkillsArray( List<String> skills ) {
        List<PolyDocument> list = skills.stream().map( s ->
                PolyDocument.ofDocument( Map.of(
                        PolyString.of( "type" ), PolyString.of( s ),
                        PolyString.of( "displayName" ), PolyString.of( s.toUpperCase( Locale.ROOT ) )
                ) )
        ).toList();
        return PolyList.of( list );
    }

}
