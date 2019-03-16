/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.util.javac;


import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;


/**
 * A <code>JavaCompilerArgs</code> holds the arguments for a {@link JavaCompiler}.
 *
 * Specific implementations of {@link JavaCompiler} may override <code>set<i>Argument</i></code> methods to store arguments in a different fashion,
 * or may throw {@link UnsupportedOperationException} to indicate that the compiler does not support that argument.
 */
public class JavaCompilerArgs {

    List<String> argsList = new ArrayList<>();
    List<String> fileNameList = new ArrayList<>();

    ClassLoader classLoader;


    public JavaCompilerArgs() {
        classLoader = getClass().getClassLoader();
    }


    public void clear() {
        fileNameList.clear();
    }


    /**
     * Sets the arguments by parsing a standard java argument string.
     *
     * A typical such string is <code>"-classpath <i>classpath</i> -d <i>dir</i> -verbose [<i>file</i>...]"</code>
     */
    public void setString( String args ) {
        List<String> list = new ArrayList<>();
        StringTokenizer tok = new StringTokenizer( args );
        while ( tok.hasMoreTokens() ) {
            list.add( tok.nextToken() );
        }
        setStringArray( list.toArray( new String[0] ) );
    }


    /**
     * Sets the arguments by parsing a standard java argument string. A typical such string is <code>"-classpath <i>classpath</i> -d <i>dir</i> -verbose [<i>file</i>...]"</code>
     */
    public void setStringArray( String[] args ) {
        for ( int i = 0; i < args.length; i++ ) {
            String arg = args[i];
            if ( arg.equals( "-classpath" ) ) {
                if ( ++i < args.length ) {
                    setClasspath( args[i] );
                }
            } else if ( arg.equals( "-d" ) ) {
                if ( ++i < args.length ) {
                    setDestdir( args[i] );
                }
            } else if ( arg.equals( "-verbose" ) ) {
                setVerbose( true );
            } else {
                argsList.add( args[i] );
            }
        }
    }


    public String[] getStringArray() {
        argsList.addAll( fileNameList );
        return argsList.toArray( new String[0] );
    }


    public void addFile( String fileName ) {
        fileNameList.add( fileName );
    }


    public String[] getFileNames() {
        return fileNameList.toArray( new String[0] );
    }


    public void setVerbose( boolean verbose ) {
        if ( verbose ) {
            argsList.add( "-verbose" );
        }
    }


    public void setDestdir( String destdir ) {
        argsList.add( "-d" );
        argsList.add( destdir );
    }


    public void setClasspath( String classpath ) {
        argsList.add( "-classpath" );
        argsList.add( classpath );
    }


    public void setDebugInfo( int i ) {
        if ( i > 0 ) {
            argsList.add( "-g=" + i );
        }
    }


    /**
     * Sets the source code (that is, the full java program, generally starting with something like "package com.foo.bar;") and the file name.
     *
     * This method is optional. It only works if the compiler supports in-memory compilation. If this compiler does not return in-memory compilation (which the base class does not), {@link #supportsSetSource}
     * returns false, and this method throws {@link UnsupportedOperationException}.
     */
    public void setSource( String source, String fileName ) {
        throw new UnsupportedOperationException();
    }


    /**
     * Returns whether {@link #setSource} will work.
     */
    public boolean supportsSetSource() {
        return false;
    }


    public void setFullClassName( String fullClassName ) {
        // NOTE jvs 28-June-2004: I added this in order to support Janino's JavaSourceClassLoader, which needs it.  Non-Farrago users don't need to call this method.
    }


    public void setClassLoader( ClassLoader classLoader ) {
        this.classLoader = classLoader;
    }


    public ClassLoader getClassLoader() {
        return classLoader;
    }
}

