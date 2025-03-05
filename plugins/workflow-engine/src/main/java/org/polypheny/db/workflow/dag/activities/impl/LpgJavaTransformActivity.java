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
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IClassBodyEvaluator;
import org.codehaus.commons.compiler.ICompilerFactory;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.entity.graph.GraphPropertyHolder;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidInputException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.LpgType;
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
import org.polypheny.db.workflow.engine.execution.pipe.LpgInputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.reader.LpgReader;
import org.polypheny.db.workflow.engine.storage.writer.LpgWriter;


@ActivityDefinition(type = "lpgJavaTransform", displayName = "Graph Java Transform", categories = { ActivityCategory.TRANSFORM, ActivityCategory.GRAPH },
        inPorts = { @InPort(type = PortType.LPG, isOptional = true, isMulti = true) },
        outPorts = { @OutPort(type = PortType.LPG) },
        shortDescription = "This activity can execute arbitrary Java code that produces a graph."
                + " WARNING: This activity is considered to be dangerous, as arbitrary code can be executed! Use it at your own risk."
)
@EnumSetting(key = "mode", displayName = "Mode", pos = 0,
        options = { "simple", "advanced" },
        displayOptions = { "Map Nodes / Edges", "Arbitrary Transform" },
        defaultValue = "simple", style = EnumStyle.RADIO_BUTTON)

@StringSetting(key = "imports", displayName = "Imports", maxLength = 10 * 1024, pos = 1,
        textEditor = true, language = "java", lineNumbers = true, defaultValue = "// import java.util.Optional;",
        shortDescription = "The most common utility classes get imported by default. Any additional imports can be specified here.")
@StringSetting(key = "simpleCode", displayName = "Transformation Code", maxLength = 10 * 1024, pos = 2,
        subPointer = "mode", subValues = { "\"simple\"" },
        textEditor = true, language = "java", nonBlank = true, lineNumbers = true,
        defaultValue = """
                int n = 1;
                int e = 1;
                
                public PolyNode transform(PolyNode node) {
                    PolyInteger nValue = PolyInteger.of(n++);
                    node.properties.put(PolyString.of("n"), nValue);
                    return node;
                }
                
                public PolyEdge transform(PolyEdge edge) {
                    PolyInteger eValue = PolyInteger.of(e++);
                    edge.properties.put(PolyString.of("e"), eValue);
                    return edge;
                }
                """,
        shortDescription = "Implementation of the 'transform(PolyNode node)' and 'transform(PolyEdge edge)' method.")
@StringSetting(key = "advancedCode", displayName = "Transformation Code", maxLength = 10 * 1024, pos = 3,
        subPointer = "mode", subValues = { "\"advanced\"" },
        textEditor = true, language = "java", nonBlank = true, lineNumbers = true,
        defaultValue = """
                public Iterator<GraphPropertyHolder> execute(final LpgReader[] inputs) throws Exception {
                    return new Iterator() {
                        final Iterator<PolyNode> nodes = inputs[0].getNodeIterator();
                        Iterator<PolyEdge> edges;
                
                        public boolean hasNext() {
                            if (edges != null) {
                                return edges.hasNext();
                            }
                            if (!nodes.hasNext()) {
                                edges = inputs[0].getEdgeIterator();
                                return edges.hasNext();
                            }
                            return true;
                        }
                
                        public GraphPropertyHolder next() {
                            return edges != null ? edges.next() : nodes.next();
                        }
                    };
                }
                """,
        shortDescription = "Implementation of the 'execute(LpgReader[] inputs)' method.")

@SuppressWarnings("unused")
public class LpgJavaTransformActivity implements Activity, Pipeable {

    private final IClassBodyEvaluator simpleCbe;
    private final IClassBodyEvaluator advancedCbe;


    public LpgJavaTransformActivity() {
        ICompilerFactory compilerFactory;
        try {
            compilerFactory = CompilerFactoryFactory.getDefaultCompilerFactory();
        } catch ( Exception e ) {
            throw new IllegalStateException( "Unable to instantiate java compiler", e );
        }
        simpleCbe = compilerFactory.newClassBodyEvaluator();
        simpleCbe.setDefaultImports( RelJavaTransformActivity.DEFAULT_IMPORTS );
        simpleCbe.setImplementedInterfaces( new Class[]{ LpgTransformable.class } );
        simpleCbe.setParentClassLoader( PolySerializable.CLASS_LOADER );

        advancedCbe = compilerFactory.newClassBodyEvaluator();
        advancedCbe.setDefaultImports( RelJavaTransformActivity.DEFAULT_IMPORTS );
        advancedCbe.setImplementedInterfaces( new Class[]{ LpgExecutable.class } );
        advancedCbe.setParentClassLoader( PolySerializable.CLASS_LOADER );
    }


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        RelJavaTransformActivity.checkAllowed();
        RelJavaTransformActivity.checkImports( settings );
        if ( settings.keysPresent( "mode" ) ) {
            boolean isSimple = settings.getString( "mode" ).equals( "simple" );
            if ( isSimple ) {
                if ( inTypes.size() > 1 || inTypes.get( 0 ).isMissing() ) {
                    throw new InvalidInputException( "In simple mode, exactly 1 input table is required.", 0 );
                }
            }

            if ( settings.allPresent() ) {
                try {
                    StringReader code = new StringReader( RelJavaTransformActivity.getCode( settings.toSettings() ) );
                    if ( isSimple ) {
                        simpleCbe.createInstance( code );
                    } else {
                        advancedCbe.createInstance( code );
                    }
                } catch ( Throwable t ) {
                    throw new InvalidSettingException( "Problem with code: " + t.getMessage(), isSimple ? "simpleCode" : "advancedCode" );
                }
            }
        }
        return LpgType.of().asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        if ( settings.getString( "mode" ).equals( "simple" ) ) {
            Pipeable.super.execute( inputs, settings, ctx );
            return;
        }
        RelJavaTransformActivity.checkAllowed();
        LpgExecutable executable = (LpgExecutable) advancedCbe.createInstance( new StringReader( RelJavaTransformActivity.getCode( settings ) ) );

        LpgWriter writer = ctx.createLpgWriter( 0 );
        LpgReader[] readers = inputs.stream().map( i -> (LpgReader) i ).toArray( LpgReader[]::new );
        Iterator<GraphPropertyHolder> iterator = executable.execute( readers );
        while ( iterator.hasNext() ) {
            writer.write( iterator.next() );
            ctx.checkInterrupted();
        }
    }


    @Override
    public Optional<Boolean> canPipe( List<TypePreview> inTypes, SettingsPreview settings ) {
        return settings.get( "mode", StringValue.class ).map( m -> m.getValue().equals( "simple" ) );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return getGraphType();
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        RelJavaTransformActivity.checkAllowed();
        LpgTransformable transformable = (LpgTransformable) simpleCbe.createInstance( new StringReader( RelJavaTransformActivity.getCode( settings ) ) );
        LpgInputPipe input = inputs.get( 0 ).asLpgInputPipe();
        for ( PolyNode node : input.getNodeIterable() ) {
            PolyNode out = transformable.transform( node );
            if ( out != null ) {
                if ( !output.put( out ) ) {
                    finish( inputs );
                    return;
                }
            }
        }
        for ( PolyEdge edge : input.getEdgeIterable() ) {
            PolyEdge out = transformable.transform( edge );
            if ( out != null ) {
                if ( !output.put( out ) ) {
                    finish( inputs );
                    return;
                }
            }
        }
    }


    public interface LpgExecutable {

        Iterator<GraphPropertyHolder> execute( final LpgReader[] inputs ) throws Exception;

    }


    public interface LpgTransformable {

        /**
         * Transforms a single node from the input graph.
         *
         * @param node the input node
         * @return the transformed node to output or null if this node should be skipped
         */
        PolyNode transform( PolyNode node ) throws Exception;

        /**
         * Transforms a single edge from the input graph.
         * After the first call to this method, no more nodes will get transformed.
         *
         * @param edge the input edge
         * @return the transformed edge to output or null if this edge should be skipped
         */
        PolyEdge transform( PolyEdge edge ) throws Exception;

    }

}
