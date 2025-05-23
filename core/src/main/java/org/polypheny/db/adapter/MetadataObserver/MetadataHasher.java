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

package org.polypheny.db.adapter.MetadataObserver;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MetadataHasher {

    private final MessageDigest digest;


    public MetadataHasher() {
        try {
            this.digest = MessageDigest.getInstance( "SHA-256" );
        } catch ( NoSuchAlgorithmException e ) {
            throw new RuntimeException( e );
        }
    }


    public String hash( String text ) {
        byte[] bytes = text.getBytes( StandardCharsets.UTF_8 );
        byte[] hash = digest.digest(bytes);

        StringBuilder sb = new StringBuilder();
        for ( byte b : hash ) {
            sb.append( String.format( "%02x", b ) );
        }
        return sb.toString();

    }


}
