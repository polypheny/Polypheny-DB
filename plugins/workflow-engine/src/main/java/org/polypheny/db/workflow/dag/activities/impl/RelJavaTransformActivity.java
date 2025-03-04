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

import java.io.StringReader;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.NotImplementedException;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IClassBodyEvaluator;
import org.codehaus.commons.compiler.ICompilerFactory;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidInputException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.UnknownType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.EnumSetting;
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.EnumSettingDef.EnumStyle;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.dag.settings.StringValue;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.engine.storage.StorageManagerImpl;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.reader.RelReader;
import org.polypheny.db.workflow.engine.storage.writer.RelWriter;


@ActivityDefinition(type = "relJavaTransform", displayName = "Relational Java Transform", categories = { ActivityCategory.TRANSFORM, ActivityCategory.RELATIONAL, ActivityCategory.DOCUMENT, ActivityCategory.GRAPH },
        inPorts = { @InPort(type = PortType.ANY, isOptional = true, isMulti = true) },
        outPorts = { @OutPort(type = PortType.ANY) },
        shortDescription = "This activity can execute arbitrary Java code that produces a tuple stream."
)
@EnumSetting(key = "mode", displayName = "Mode", pos = 0,
        options = { "simple", "advanced" },
        displayOptions = { "Row-Wise Transform", "Arbitrary Transform" },
        defaultValue = "simple", style = EnumStyle.RADIO_BUTTON)

@StringSetting(key = "imports", displayName = "Imports", maxLength = 10 * 1024, pos = 1,
        textEditor = true, language = "java", lineNumbers = true, defaultValue = "// import java.util.Optional;",
        shortDescription = "The most common utility classes get imported by default. Any additional imports can be specified here.")
@StringSetting(key = "simpleCode", displayName = "Transformation Code", maxLength = 10 * 1024, pos = 2,
        subPointer = "mode", subValues = { "\"simple\"" },
        textEditor = true, language = "java", nonBlank = true, lineNumbers = true,
        defaultValue = """
                int n = 1;
                
                public PolyValue[] transform(PolyValue[] row) {
                    PolyInteger nValue = PolyInteger.of(n++);
                    row[row.length - 1] = row[row.length - 1].asNumber().multiply(nValue);
                    return row;
                }
                
                
                
                /* Uncomment to set an outType that differs from the inType
                public AlgDataType getOutType(AlgDataType inType) {
                    return factory.builder()
                        .addAll(inType.getFields())
                        .add("new_col", null, PolyType.INTEGER)
                        .build();
                }*/
                """,
        shortDescription = "Implementation of the 'transform(PolyValue[] row)' method.")
@StringSetting(key = "advancedCode", displayName = "Transformation Code", maxLength = 10 * 1024, pos = 3,
        subPointer = "mode", subValues = { "\"advanced\"" },
        textEditor = true, language = "java", nonBlank = true, lineNumbers = true,
        defaultValue = """
                public Iterator<PolyValue[]> execute(final RelReader[] inputs) throws Exception {
                    return new Iterator() {
                        final Iterator<PolyValue[]> reader = inputs[0].getArrayIterator();
                
                        @Override
                        public boolean hasNext() {
                            return reader.hasNext();
                        }
                
                        @Override
                        public PolyValue[] next() {
                            return (PolyValue[]) reader.next();
                        }
                    };
                }
                
                
                
                /* Uncomment to set an outType that differs from the first inType
                public AlgDataType getOutType(AlgDataType[] inTypes) {
                    return factory.builder().add(PK_FIELD) // first field must always be PK_FIELD
                            .add("data", null, PolyType.INTEGER)
                            .build();
                } */
                """,
        shortDescription = "Implementation of the 'execute(final RelReader[] inputs)' method.")

@SuppressWarnings("unused")
public class RelJavaTransformActivity implements Activity, Pipeable {

    public static final String[] DEFAULT_IMPORTS = new String[]{
            "java.util.ArrayList",
            "java.util.LinkedList",
            "java.util.List",
            "java.util.Arrays",
            "java.util.Iterator",
            "java.util.Set",
            "java.util.HashSet",
            "java.util.LinkedHashSet",
            "java.util.TreeSet",
            "java.util.Map",
            "java.util.HashMap",
            "java.util.LinkedHashMap",
            "java.util.TreeMap",
            "java.util.Collection",
            "java.util.Collections",
            "java.util.Objects",
            "java.util.Comparator",
            "java.util.function.Function",
            "java.util.function.Predicate",
            "java.util.function.Supplier",
            "java.util.stream.Stream",
            "java.util.stream.Collectors",
            "java.util.stream.IntStream",
            "java.lang.Math",
            "org.polypheny.db.algebra.type.*",
            "org.polypheny.db.type.PolyType",
            "org.polypheny.db.type.entity.*",
            "org.polypheny.db.type.entity.document.PolyDocument",
            "org.polypheny.db.type.entity.graph.*",
            "org.polypheny.db.type.entity.numerical.*",
            "org.polypheny.db.type.entity.relational.PolyMap",
            "org.polypheny.db.type.entity.temporal.*",
            "org.polypheny.db.workflow.dag.activities.ActivityUtils",
            "org.polypheny.db.workflow.engine.storage.QueryUtils",
            "org.polypheny.db.workflow.engine.storage.reader.CheckpointQuery",
            "org.polypheny.db.workflow.engine.storage.reader.RelReader",
            "org.polypheny.db.workflow.engine.storage.StorageManager"
    };
    private IClassBodyEvaluator simpleCbe;
    private IClassBodyEvaluator advancedCbe;


    public RelJavaTransformActivity() {
        ICompilerFactory compilerFactory;
        try {
            compilerFactory = CompilerFactoryFactory.getDefaultCompilerFactory();
        } catch ( Exception e ) {
            throw new IllegalStateException( "Unable to instantiate java compiler", e );
        }
        simpleCbe = compilerFactory.newClassBodyEvaluator();
        simpleCbe.setDefaultImports( DEFAULT_IMPORTS );
        simpleCbe.setImplementedInterfaces( new Class[]{ Transformable.class } );
        simpleCbe.setParentClassLoader( PolySerializable.CLASS_LOADER );

        advancedCbe = compilerFactory.newClassBodyEvaluator();
        advancedCbe.setDefaultImports( DEFAULT_IMPORTS );
        advancedCbe.setImplementedInterfaces( new Class[]{ Executable.class } );
        advancedCbe.setParentClassLoader( PolySerializable.CLASS_LOADER );
    }


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        if ( settings.keysPresent( "imports" ) ) {
            String imports = settings.getString( "imports" );
            if ( !imports.isBlank() && !Arrays.stream( imports.trim().split( "\n" ) )
                    .map( String::trim )
                    .allMatch( line -> line.isBlank() || line.startsWith( "//" ) || line.startsWith( "import" ) ) ) {
                throw new InvalidSettingException( "Only valid Java imports are allowed", "imports" );
            }
        }
        if ( settings.keysPresent( "mode" ) ) {
            boolean isSimple = settings.getString( "mode" ).equals( "simple" );
            if ( isSimple ) {
                if ( inTypes.size() > 1 || inTypes.get( 0 ).isMissing() ) {
                    throw new InvalidInputException( "In simple mode, exactly 1 input table is required.", 0 );
                }
            }

            if ( inTypes.stream().allMatch( TypePreview::isPresent ) && settings.allPresent() ) {
                try {
                    AlgDataType type;
                    StringReader code = new StringReader( getCode( settings.toSettings() ) );
                    if ( isSimple ) {
                        Transformable transformable = (Transformable) simpleCbe.createInstance( code );
                        //type = transformable.getOutType( inTypes.get( 0 ).getNullableType() );
                    } else {
                        Executable executable = (Executable) advancedCbe.createInstance( code );
                        //type = executable.getOutType( inTypes.stream().map( TypePreview::getNullableType ).toList() );
                    }
                    // automatic execution of getOutType is disabled on purpose for safety
                    //return RelType.of( type ).asOutTypes();
                } catch ( Throwable t ) {
                    throw new InvalidSettingException( "Problem with code: " + t.getMessage(), isSimple ? "simpleCode" : "advancedCode" );
                }
            }
        }

        return UnknownType.ofRel().asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        if ( settings.getString( "mode" ).equals( "simple" ) ) {
            Pipeable.super.execute( inputs, settings, ctx );
            return;
        }
        Executable executable = (Executable) advancedCbe.createInstance( new StringReader( getCode( settings ) ) );
        AlgDataType[] inTypes = inputs.stream().map( i -> i == null ? null : i.getTupleType() ).toArray(AlgDataType[]::new);
        AlgDataType outType = executable.getOutType( inTypes );
        int fieldCount = outType.getFieldCount();

        RelWriter writer = ctx.createRelWriter( 0, outType  );
        RelReader[] readers = inputs.stream().map( i -> (RelReader) i ).toArray(RelReader[]::new);
        Iterator<PolyValue[]> iterator = executable.execute( readers );
        while ( iterator.hasNext() ) {
            PolyValue[] row = iterator.next();
            if ( row.length != fieldCount ) {
                throw new GenericRuntimeException( "Number of columns in the transformed row does not match the expected type: " + row.length + " != " + fieldCount );
            }
            writer.write( Arrays.asList( row ) );
            ctx.checkInterrupted();
        }
    }


    @Override
    public Optional<Boolean> canPipe( List<TypePreview> inTypes, SettingsPreview settings ) {
        return settings.get( "mode", StringValue.class ).map( m -> m.getValue().equals( "simple" ) );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        Transformable transformable = (Transformable) simpleCbe.createInstance( new StringReader( getCode( settings ) ) );
        return transformable.getOutType( inTypes.get( 0 ) );
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        Transformable transformable = (Transformable) simpleCbe.createInstance( new StringReader( getCode( settings ) ) );
        int fieldCount = output.getType().getFieldCount();
        for ( List<PolyValue> row : inputs.get( 0 ) ) {
            PolyValue[] outRow = transformable.transform( row.toArray( new PolyValue[0] ) );
            if ( outRow != null ) {
                if ( outRow.length != fieldCount ) {
                    throw new GenericRuntimeException( "Number of columns in the transformed row does not match the expected type: " + outRow.length + " != " + fieldCount );
                }
                if ( !output.put( Arrays.asList( outRow ) ) ) {
                    finish( inputs );
                    return;
                }
            }
        }
    }


    private String getCode( Settings settings ) {
        String imports = settings.getString( "imports" );
        if ( settings.getString( "mode" ).equals( "simple" ) ) {
            return imports + "\n" + settings.getString( "simpleCode" );
        }
        return imports + "\n" + settings.getString( "advancedCode" );
    }


    public interface Executable {

        AlgDataTypeFactory factory = AlgDataTypeFactory.DEFAULT;
        AlgDataTypeField PK_FIELD = StorageManagerImpl.PK_FIELD;

        default AlgDataType getOutType( AlgDataType[] inTypes ) {
            if ( inTypes.length == 1 && inTypes[0] != null ) {
                return inTypes[0];
            }
            throw new NotImplementedException( "getOutType( List<AlgDataType> inTypes ) is not implemented" );
        }

        Iterator<PolyValue[]> execute( final RelReader[] inputs ) throws Exception;

    }


    public interface Transformable {

        AlgDataTypeFactory factory = AlgDataTypeFactory.DEFAULT;
        AlgDataTypeField PK_FIELD = StorageManager.PK_FIELD;

        default AlgDataType getOutType( AlgDataType inType ) {
            return inType;
        }

        /**
         * Transforms a single row from the input table.
         *
         * @param row the input row
         * @return the row to output with values compatible to getOutType() or null if this row should be skipped
         */
        PolyValue[] transform( PolyValue[] row ) throws Exception;

    }


    public static class ExecutableImpl implements Executable {


        public Iterator<PolyValue[]> execute( RelReader[] inputs ) throws Exception {

            return new Iterator() {
                final Iterator<PolyValue[]> reader = inputs[0].getArrayIterator();


                @Override
                public boolean hasNext() {
                    return reader.hasNext();
                }


                @Override
                public PolyValue[] next() {
                    return (PolyValue[]) reader.next();
                }
            };
        }


        /* Uncomment to set an outType that differs from the first inType */
        public AlgDataType getOutType( AlgDataType[] inTypes ) {
            return factory.builder().add( Executable.PK_FIELD ) // first field must always be PK_FIELD
                    .add( "data", null, PolyType.INTEGER )
                    .build();
        }

    }

}
