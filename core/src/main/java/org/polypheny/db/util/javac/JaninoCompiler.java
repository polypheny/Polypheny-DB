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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.util.javac;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.codehaus.janino.JavaSourceClassLoader;
import org.codehaus.janino.util.ClassFile;
import org.codehaus.janino.util.resource.MapResourceFinder;
import org.codehaus.janino.util.resource.ResourceFinder;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.config.RuntimeConfig;


/**
 * <code>JaninoCompiler</code> implements the {@link JavaCompiler} interface by calling <a href="http://www.janino.net">Janino</a>.
 */
public class JaninoCompiler implements JavaCompiler {

    public JaninoCompilerArgs args = new JaninoCompilerArgs();

    // REVIEW jvs:  pool this instance?  Is it thread-safe?
    private AccountingClassLoader classLoader;


    public JaninoCompiler() {
    }


    // implement JavaCompiler
    @Override
    public void compile() {
        // REVIEW: SWZ: 3/12/2006: When this method is invoked multiple times, it creates a series of AccountingClassLoader objects, each with the previous as its parent ClassLoader.  If we refactored this
        // class and its callers to specify all code to compile in one go, we could probably just use a single AccountingClassLoader.

        assert args.destdir != null;
        assert args.fullClassName != null;
        assert args.source != null;

        ClassLoader parentClassLoader = args.getClassLoader();
        if ( classLoader != null ) {
            parentClassLoader = classLoader;
        }

        Map<String, byte[]> sourceMap = new HashMap<>();
        sourceMap.put(
                ClassFile.getSourceResourceName( args.fullClassName ),
                args.source.getBytes( StandardCharsets.UTF_8 ) );
        MapResourceFinder sourceFinder = new MapResourceFinder( sourceMap );

        classLoader =
                new AccountingClassLoader(
                        parentClassLoader,
                        sourceFinder,
                        null,
                        args.destdir == null
                                ? null
                                : new File( args.destdir ) );
        if ( RuntimeConfig.DEBUG.getBoolean() ) {
            // Add line numbers to the generated janino class
            classLoader.setDebuggingInfo( true, true, true );
        }
        try {
            classLoader.loadClass( args.fullClassName );
        } catch ( ClassNotFoundException ex ) {
            throw new GenericRuntimeException( "while compiling " + args.fullClassName, ex );
        }
    }


    // implement JavaCompiler
    @Override
    public JavaCompilerArgs getArgs() {
        return args;
    }


    // implement JavaCompiler
    @Override
    public ClassLoader getClassLoader() {
        return classLoader;
    }


    // implement JavaCompiler
    @Override
    public int getTotalByteCodeSize() {
        return classLoader.getTotalByteCodeSize();
    }


    /**
     * Arguments to an invocation of the Janino compiler.
     */
    public static class JaninoCompilerArgs extends JavaCompilerArgs {

        String destdir;
        String fullClassName;
        String source;


        public JaninoCompilerArgs() {
        }


        @Override
        public boolean supportsSetSource() {
            return true;
        }


        @Override
        public void setDestdir( String destdir ) {
            super.setDestdir( destdir );
            this.destdir = destdir;
        }


        @Override
        public void setSource( String source, String fileName ) {
            this.source = source;
            addFile( fileName );
        }


        @Override
        public void setFullClassName( String fullClassName ) {
            this.fullClassName = fullClassName;
        }
    }


    /**
     * Refinement of JavaSourceClassLoader which keeps track of the total bytecode length of the classes it has compiled.
     */
    private static class AccountingClassLoader extends JavaSourceClassLoader {

        private final File destDir;
        private int nBytes;


        AccountingClassLoader( ClassLoader parentClassLoader, ResourceFinder sourceFinder, String optionalCharacterEncoding, File destDir ) {
            super( parentClassLoader, sourceFinder, optionalCharacterEncoding );
            this.destDir = destDir;
        }


        int getTotalByteCodeSize() {
            return nBytes;
        }


        @Override
        public Map<String, byte[]> generateBytecodes( String name ) throws ClassNotFoundException {
            final Map<String, byte[]> map = super.generateBytecodes( name );
            if ( map == null ) {
                return null;
            }

            if ( destDir != null ) {
                try {
                    for ( Map.Entry<String, byte[]> entry : map.entrySet() ) {
                        File file = new File( destDir, entry.getKey() + ".class" );
                        FileOutputStream fos = new FileOutputStream( file );
                        fos.write( entry.getValue() );
                        fos.close();
                    }
                } catch ( IOException e ) {
                    throw new GenericRuntimeException( e );
                }
            }

            // NOTE jvs:  Janino has actually compiled everything to bytecode even before all of the classes have actually been loaded.  So we intercept their sizes here just
            // after they've been compiled.
            for ( Object obj : map.values() ) {
                byte[] bytes = (byte[]) obj;
                nBytes += bytes.length;
            }
            return map;
        }
    }
}

