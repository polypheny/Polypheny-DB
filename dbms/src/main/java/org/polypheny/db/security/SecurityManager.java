/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.security;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;
import org.polypheny.db.webui.Crud;

public class SecurityManager {

    private static SecurityManager INSTANCE = null;
    private final Crud crud;


    private SecurityManager( Crud crud ) {
        this.crud = crud;
    }


    public static SecurityManager build( Crud crud ) {
        if ( INSTANCE != null ) {
            throw new SecurityException( "Overwriting SecurityManager was blocked." );
        }
        INSTANCE = new SecurityManager( crud );
        return INSTANCE;
    }


    public static SecurityManager getInstance() {
        return INSTANCE;
    }


    public boolean uiAccessPossible( URL csvDir ) {
        // generate UUID
        UUID uuid = UUID.randomUUID();
        // send UUID to Polypheny-UI

        Callback callback = crud.createAccessFile( uuid, csvDir );
        // wait for response from Polypheny-UI

        // check if file in folder
        boolean accessGranted = Arrays.stream( Objects.requireNonNull( new File( csvDir.getPath() ).listFiles() ) ).anyMatch( f -> f.getName().equals( ".polypheny-access" ) && f.isFile() );
        if ( !accessGranted ) {

        }
    }

}
