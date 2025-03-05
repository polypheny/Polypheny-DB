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
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidInputException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.DocType;
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
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.reader.DocReader;
import org.polypheny.db.workflow.engine.storage.writer.DocWriter;


@ActivityDefinition(type = "docJavaTransform", displayName = "Document Java Transform", categories = { ActivityCategory.TRANSFORM, ActivityCategory.DOCUMENT },
        inPorts = { @InPort(type = PortType.DOC, isOptional = true, isMulti = true) },
        outPorts = { @OutPort(type = PortType.DOC) },
        shortDescription = "This activity can execute arbitrary Java code that produces a document stream."
                + " WARNING: This activity is considered to be dangerous, as arbitrary code can be executed! Use it at your own risk."
)
@EnumSetting(key = "mode", displayName = "Mode", pos = 0,
        options = { "simple", "advanced" },
        displayOptions = { "Map Documents", "Arbitrary Transform" },
        defaultValue = "simple", style = EnumStyle.RADIO_BUTTON)

@StringSetting(key = "imports", displayName = "Imports", maxLength = 10 * 1024, pos = 1,
        textEditor = true, language = "java", lineNumbers = true, defaultValue = "// import java.util.Optional;",
        shortDescription = "The most common utility classes get imported by default. Any additional imports can be specified here.")
@StringSetting(key = "simpleCode", displayName = "Transformation Code", maxLength = 10 * 1024, pos = 2,
        subPointer = "mode", subValues = { "\"simple\"" },
        textEditor = true, language = "java", nonBlank = true, lineNumbers = true,
        defaultValue = """
                int n = 1;
                
                public PolyDocument transform(PolyDocument doc) {
                    PolyInteger nValue = PolyInteger.of(n++);
                    doc.put(PolyString.of("n"), nValue);
                    return doc;
                }
                """,
        shortDescription = "Implementation of the 'transform(PolyDocument doc)' method.")
@StringSetting(key = "advancedCode", displayName = "Transformation Code", maxLength = 10 * 1024, pos = 3,
        subPointer = "mode", subValues = { "\"advanced\"" },
        textEditor = true, language = "java", nonBlank = true, lineNumbers = true,
        defaultValue = """
                public Iterator<PolyDocument> execute(final DocReader[] inputs) throws Exception {
                    return new Iterator() {
                        final Iterator<PolyDocument> reader = inputs[0].getDocIterator();
                
                        public boolean hasNext() {
                            return reader.hasNext();
                        }
                
                        public PolyDocument next() {
                            return (PolyDocument) reader.next();
                        }
                    };
                }
                """,
        shortDescription = "Implementation of the 'execute(final DocReader[] inputs)' method.")

@SuppressWarnings("unused")
public class DocJavaTransformActivity implements Activity, Pipeable {

    private final IClassBodyEvaluator simpleCbe;
    private final IClassBodyEvaluator advancedCbe;


    public DocJavaTransformActivity() {
        ICompilerFactory compilerFactory;
        try {
            compilerFactory = CompilerFactoryFactory.getDefaultCompilerFactory();
        } catch ( Exception e ) {
            throw new IllegalStateException( "Unable to instantiate java compiler", e );
        }
        simpleCbe = compilerFactory.newClassBodyEvaluator();
        simpleCbe.setDefaultImports( RelJavaTransformActivity.DEFAULT_IMPORTS );
        simpleCbe.setImplementedInterfaces( new Class[]{ DocTransformable.class } );
        simpleCbe.setParentClassLoader( PolySerializable.CLASS_LOADER );

        advancedCbe = compilerFactory.newClassBodyEvaluator();
        advancedCbe.setDefaultImports( RelJavaTransformActivity.DEFAULT_IMPORTS );
        advancedCbe.setImplementedInterfaces( new Class[]{ DocExecutable.class } );
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
        return DocType.of().asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        if ( settings.getString( "mode" ).equals( "simple" ) ) {
            Pipeable.super.execute( inputs, settings, ctx );
            return;
        }
        RelJavaTransformActivity.checkAllowed();
        DocExecutable executable = (DocExecutable) advancedCbe.createInstance( new StringReader( RelJavaTransformActivity.getCode( settings ) ) );

        DocWriter writer = ctx.createDocWriter( 0 );
        DocReader[] readers = inputs.stream().map( i -> (DocReader) i ).toArray( DocReader[]::new );
        Iterator<PolyDocument> iterator = executable.execute( readers );
        while ( iterator.hasNext() ) {
            PolyDocument doc = iterator.next();
            writer.write( doc );
            ctx.checkInterrupted();
        }
    }


    @Override
    public Optional<Boolean> canPipe( List<TypePreview> inTypes, SettingsPreview settings ) {
        return settings.get( "mode", StringValue.class ).map( m -> m.getValue().equals( "simple" ) );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return getDocType();
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        RelJavaTransformActivity.checkAllowed();
        DocTransformable transformable = (DocTransformable) simpleCbe.createInstance( new StringReader( RelJavaTransformActivity.getCode( settings ) ) );
        for ( List<PolyValue> tuple : inputs.get( 0 ) ) {
            PolyDocument doc = transformable.transform( tuple.get( 0 ).asDocument() );
            if ( doc != null ) {
                if ( !output.put( doc ) ) {
                    finish( inputs );
                    return;
                }
            }
        }
    }


    public interface DocExecutable {

        Iterator<PolyDocument> execute( final DocReader[] inputs ) throws Exception;

    }


    public interface DocTransformable {

        /**
         * Transforms a single document from the input collection.
         *
         * @param doc the input document
         * @return the transformed document to output or null if this document should be skipped
         */
        PolyDocument transform( PolyDocument doc ) throws Exception;

    }

}
