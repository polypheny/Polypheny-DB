/*
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
 *
 */

package ch.unibas.dmi.dbis.polyphenydb;


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
