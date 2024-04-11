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

package org.polypheny.db.util;

import java.io.PrintWriter;
import java.util.Set;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic;

@SupportedAnnotationTypes("OptionalGetter")
@SupportedSourceVersion(SourceVersion.RELEASE_11)
public class PolyProcessor extends AbstractProcessor {

    @Override
    public synchronized void init( ProcessingEnvironment processingEnv ) {
        super.init( processingEnv );
        System.out.println( "PolyProcessor initialized");
    }


    @Override
    public boolean process( Set<? extends TypeElement> annotations, RoundEnvironment roundEnv ) {
        for ( Element element : roundEnv.getElementsAnnotatedWith( OptionalGetter.class ) ) {
            if ( element.getKind() == ElementKind.FIELD ) {
                generateOptionalGetter( (TypeElement) element.getEnclosingElement(), element );
            } else {
                processingEnv.getMessager().printMessage( Diagnostic.Kind.ERROR, "Only fields can be annotated with @OptionalGetter", element );
            }
        }
        return true;
    }


    private void generateOptionalGetter( TypeElement classElement, Element fieldElement ) {
        String fieldName = fieldElement.getSimpleName().toString();
        TypeMirror fieldType = fieldElement.asType();
        String className = classElement.getSimpleName().toString();
        String getterName = "get" + capitalize( fieldName );

        processingEnv.getMessager().printMessage( Diagnostic.Kind.NOTE, "Generating optional-wrapped getter for " + fieldName + " in " + className );

        PrintWriter writer = null;
        try {
            writer = new PrintWriter( processingEnv.getFiler().createSourceFile( className + "OptionalGetters" ).openWriter() );

            // Package
            writer.println( "package " + classElement.getEnclosingElement().toString() + ";" );
            writer.println();

            // Imports
            writer.println( "import java.util.Optional;" );
            writer.println();

            // Class declaration
            writer.println( "public class " + className + "OptionalGetters {" );
            writer.println();

            // Getter method
            writer.println( "    public Optional<" + fieldType + "> " + getterName + "() {" );
            writer.println( "        return Optional.ofNullable(this." + fieldName + ");" );
            writer.println( "    }" );
            writer.println();

            // End of class
            writer.println( "}" );

        } catch ( Exception e ) {
            e.printStackTrace();
            processingEnv.getMessager().printMessage( Diagnostic.Kind.ERROR, "Error generating optional-wrapped getter for " + fieldName + " in " + className );
        } finally {
            if ( writer != null ) {
                writer.close();
            }
        }
    }


    private String capitalize( String str ) {
        return str.substring( 0, 1 ).toUpperCase() + str.substring( 1 );
    }

}
