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

package org.polypheny.db.workflow.dag.settings;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import lombok.Value;
import org.polypheny.db.util.Source;
import org.polypheny.db.util.Sources;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue;

@Value
public class FileValue implements SettingValue {

    String path;
    SourceType type;
    boolean multi;


    public void validate( boolean allowMultiple ) {
        if ( multi && !(allowMultiple && type.allowMultiple) ) {
            throw new IllegalArgumentException( "Multiple files are not allowed" );
        }
        if ( path.isBlank() ) {
            throw new IllegalArgumentException( "Path must not be empty" );
        }
    }


    @JsonIgnore
    public Source getSource() throws IOException {
        switch ( type ) {
            case ABS_FILE:
                if ( path.startsWith( "classpath://" ) ) {
                    return getClassPathSource( path.substring( "classpath://".length() ) );
                }
                File absFile = new File( path );
                if ( !absFile.exists() || !absFile.isFile() ) {
                    throw new FileNotFoundException( "File not found: " + path );
                }
                return Sources.of( absFile );
            case REL_FILE: // TODO: change to something actually useful. e.g. relative to .polypheny
                return getClassPathSource( path );
            case URL:
                return Sources.of( new URL( path ) );
        }
        throw new IllegalArgumentException( "Unsupported type: " + type );
    }


    @JsonIgnore
    public List<Source> getSources() throws IOException {
        return getSources( Set.of() );
    }


    @JsonIgnore
    public List<Source> getSources( Set<String> allowedExtensions ) throws IOException {
        if ( !isMulti() ) {
            return List.of( getSource() );
        }

        File dir = new File( path );
        if ( !dir.isDirectory() ) {
            throw new IOException( "Expected a directory, but got: " + path );
        }

        File[] files = dir.listFiles();
        if ( files == null ) {
            throw new IOException( "Unable to list files in: " + path );
        }

        return Arrays.stream( files )
                .filter( File::isFile ) // Ignore subdirectories
                .filter( file -> allowedExtensions.isEmpty() || allowedExtensions.stream().anyMatch(
                        ext -> file.getName().endsWith( "." + ext.toLowerCase( Locale.ROOT ) ) ) )
                .map( Sources::of )
                .toList();
    }


    private static Source getClassPathSource( String path ) throws FileNotFoundException {
        URL resource = FileValue.class.getClassLoader().getResource( path );
        if ( resource == null ) {
            throw new FileNotFoundException( "Resource not found: " + path );
        }
        return Sources.of( resource );
    }


    public enum SourceType {
        ABS_FILE( true ),
        REL_FILE( true ), // use this.getClass().getClassLoader().getResource
        URL( false );

        final boolean allowMultiple;


        SourceType( boolean allowMultiple ) {
            this.allowMultiple = allowMultiple;
        }
    }

}
