/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.adapter.parquet.shared.io;

import java.net.MalformedURLException;
import java.net.URL;

/**
 *  Parquet file/directory URL handling
 */
public final class ParquetUrlResolver {

    private ParquetUrlResolver() {
    }


    public static URL asDirectoryUrl( URL url ) {
        if ( url == null || url.toExternalForm().endsWith( "/" ) ) {
            return url;
        }

        try {
            return new URL( url.toExternalForm() + "/" );
        } catch ( MalformedURLException e ) {
            throw new org.polypheny.db.catalog.exceptions.GenericRuntimeException( e );
        }
    }


    public static URL asSourceUrl( URL url ) {
        if ( url == null || url.getPath().toLowerCase().endsWith( ".parquet" ) ) {
            return url;
        }
        return asDirectoryUrl( url );
    }


    public static URL resolveFile( URL directoryUrl, String fileName ) {
        if ( directoryUrl != null && directoryUrl.getPath().toLowerCase().endsWith( ".parquet" ) ) {
            return directoryUrl;
        }
        try {
            return new URL( asDirectoryUrl( directoryUrl ), fileName );
        } catch ( MalformedURLException e ) {
            throw new org.polypheny.db.catalog.exceptions.GenericRuntimeException( e );
        }
    }

}
