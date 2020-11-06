/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.webui;


import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.stream.Collectors;
import org.polypheny.db.util.Pair;


/**
 * Manager that deletes temporary files at a scheduled fixed rate
 */
public class TemporalFileManager {

    private static final Map<String, TemporalFileManager> managers = new HashMap<>();
    private final File root;
    /**
     * List of absolute paths of files and when they were created
     */
    private final List<Pair<Long, String>> files = new LinkedList<>();


    public static TemporalFileManager getInstance( File rootFile ) {
        if ( !managers.containsKey( rootFile.getAbsolutePath() ) ) {
            managers.put( rootFile.getAbsolutePath(), new TemporalFileManager( rootFile ) );
        }
        return managers.get( rootFile.getAbsolutePath() );
    }


    private TemporalFileManager( final File root ) {
        this.root = root;
        Timer timer = new Timer();
        timer.scheduleAtFixedRate( new TimerTask() {
            @Override
            public void run() {
                deleteOlderFiles();
            }
        }, 60_000, 60_000 );
        initOnShutdown();
    }


    private void initOnShutdown() {
        Runtime.getRuntime().addShutdownHook( new Thread( () -> {
            for ( String absPath : files.stream().map( f -> f.right ).collect( Collectors.toList() ) ) {
                new File( absPath ).delete();
            }
        } ) );
    }


    /**
     * Add a file to the manager, so it will be deleted after a timeout
     * The file will only be added to the manager if it exists
     */
    public boolean addFile( final File file ) {
        File f = new File( root, file.getName() );
        if ( f.exists() ) {
            files.add( new Pair<>( System.currentTimeMillis(), f.getAbsolutePath() ) );
        }
        return f.exists();
    }


    /**
     * Add a file to the manager, so it will be deleted after a timeout
     * The file will only be added to the manager if it exists
     */
    public boolean addPath( final Path path ) {
        File f = new File( path.toString() );
        return addFile( f );
    }


    private void deleteOlderFiles() {
        while ( System.currentTimeMillis() - files.get( 0 ).left > 300_000 ) {
            File f = new File( files.get( 0 ).right );
            f.delete();
            files.remove( 0 );
        }
    }

}
