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

package org.polypheny.db.transaction;


public class Utils {


    /**
     * Identifier for a global transaction (includes already the branch part for this branch)
     *
     * Scope: GLOBAL, local (i.e., the origin of this distributed transaction)
     */
    public static PolyXid generateGlobalTransactionIdentifier( final PUID nodeId, final PUID userId, final PUID connectionId, final PUID transactionId ) {
        return PolyXid.generateGlobalTransactionIdentifier( nodeId, userId, connectionId, transactionId );
    }


    /**
     * Identifier which needs to be generated on a remote branch out of the global transaction identifier
     *
     * Scope: GLOBAL, remote branch (i.e., this branch is not the origin of this distributed transaction
     */
    public static PolyXid generateBranchTransactionIdentifier( final PolyXid globalTransactionIdentifier, final PUID nodeId ) {
        return PolyXid.generateBranchTransactionIdentifier( globalTransactionIdentifier, nodeId );
    }


    /**
     * Identifier which is used for locally running -- mostly maintenance -- transactions
     *
     * This identifier has only the transaction id part of the global part set, as well as the node part of the branch part.
     *
     * Scope: LOCAL
     */
    public static PolyXid generateLocalTransactionIdentifier( final PUID nodeId, final PUID transactionId ) {
        return PolyXid.generateLocalTransactionIdentifier( nodeId, transactionId );
    }


    /*
     * From https://stackoverflow.com/questions/4485128/how-do-i-convert-long-to-byte-and-back-in-java/29132118#29132118
     */
    public static byte[] longToBytes( long l ) {
        byte[] result = new byte[Long.BYTES];
        for ( int i = (Long.BYTES - 1); i >= 0; i-- ) {
            result[i] = (byte) (l & 0xFF);
            l >>= Byte.SIZE;
        }
        return result;
    }


    /*
     * From https://stackoverflow.com/questions/4485128/how-do-i-convert-long-to-byte-and-back-in-java/29132118#29132118
     */
    public static long bytesToLong( byte[] b ) {
        long result = 0;
        for ( int i = 0; i < Long.BYTES; i++ ) {
            result <<= Byte.SIZE;
            result |= (b[i] & 0xFF);
        }
        return result;
    }


    private Utils() {
        throw new IllegalAccessError( "This is a utility class. No instances for you." );
    }
}
