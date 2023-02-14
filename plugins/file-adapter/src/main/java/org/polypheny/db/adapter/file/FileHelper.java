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

package org.polypheny.db.adapter.file;


import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Comparator;


public class FileHelper {

    static void deleteDirRecursively( final File dir ) throws IOException {
        //from https://www.baeldung.com/java-delete-directory
        Files.walk( dir.toPath() )
                .sorted( Comparator.reverseOrder() )
                .map( Path::toFile )
                .forEach( File::delete );
    }

    ///
    // DATE / TIME / TIMESTAMP HELPER FUNCTIONS
    //


    static boolean isSqlDateOrTimeOrTS( final Object o ) {
        return (o instanceof Date) || (o instanceof Time) || (o instanceof Timestamp);
    }


    static Long sqlToLong( final Object o ) {
        if ( o instanceof Time ) {
            return sqlToLong( (Time) o );
        } else if ( o instanceof Date ) {
            return sqlToLong( (Date) o );
        } else if ( o instanceof Timestamp ) {
            return sqlToLong( (Timestamp) o );
        }
        throw new IllegalArgumentException( "Unexpected input, must be SQL Time, Date or Timestamp" );
    }


    static Long sqlToLong( final Time time ) {
        return time.getTime();
    }


    static Long sqlToLong( final Date date ) {
        return date.toLocalDate().toEpochDay();
    }


    static Long sqlToLong( final Timestamp timestamp ) {
        return timestamp.toInstant().toEpochMilli();
    }

}
