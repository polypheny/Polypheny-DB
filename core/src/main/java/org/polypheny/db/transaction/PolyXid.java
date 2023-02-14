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

package org.polypheny.db.transaction;


import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Arrays;
import lombok.EqualsAndHashCode;
import org.apache.commons.codec.binary.Hex;


/**
 *
 */
@EqualsAndHashCode(doNotUseGetters = true)
public class PolyXid implements javax.transaction.xa.Xid, Serializable {

    private static final long serialVersionUID = 1L;

    // LSB = least significant bit/byte
    // MSB = most significant bit/byte

    private static final int NODE_ID_LSB_INDEX = 48;
    private static final int USER_ID_LSB_INDEX = 32;
    private static final int CONNECTION_ID_LSB_INDEX = 16;
    private static final int TRANSACTION_ID_LSB_INDEX = 0;
    private static final int STORE_ID_LSB_INDEX = 32;
    private static final int RESERVED_ID_LSB_INDEX = 16;
    private static final int CUSTOM_ID_LSB_INDEX = 0;

    private final int formatId;
    private final byte[] globalTransactionId = new byte[javax.transaction.xa.Xid.MAXGTRIDSIZE];
    private final byte[] branchQualifier = new byte[javax.transaction.xa.Xid.MAXBQUALSIZE];


    /**
     * Identifier for a global transaction (includes already the branch part for this branch)
     *
     * Scope: GLOBAL, local (i.e., the origin of this distributed transaction)
     */
    public static PolyXid generateGlobalTransactionIdentifier( final PUID nodeId, final PUID userId, final PUID connectionId, final PUID transactionId ) {
        return new PolyXid( nodeId, userId, connectionId, transactionId );
    }


    /**
     * Identifier which needs to be generated on a remote branch out of the global transaction identifier
     *
     * Scope: GLOBAL, remote branch (i.e., this branch is not the origin of this distributed transaction
     */
    public static PolyXid generateBranchTransactionIdentifier( final PolyXid globalTransactionIdentifier, final PUID nodeId ) {
        return new PolyXid( globalTransactionIdentifier, nodeId );
    }


    /**
     * Identifier which is used for locally running -- mostly maintenance -- transactions
     *
     * This identifier has only the transaction id part of the global part set, as well as the node part of the branch part.
     *
     * Scope: LOCAL
     */
    public static PolyXid generateLocalTransactionIdentifier( final PUID nodeId, final PUID transactionId ) {
        return new PolyXid( nodeId, transactionId );
    }


    /*
     *
     *         |-------------------- 64 Bytes ---------------------|
     * PUID:     |- 16 B -|
     * long:      |-8-|
     *
     * GLOBAL: [  (nodeId)  (userId)  (connectionId) (transactionId) ]
     * index:      56   48   40   32       24     16       8       0
     * BRANCH: [  (nodeId)  (storeId)  (reserved)      (custom)      ]
     *
     *
     *
     * PolyXid:  |- formatId (4 Bytes) -|   |----------------- globalTransactionId (64 Bytes) --------------------------------------------------------|   |----------------- branchQualifier (64 Bytes) ------------------------------------------------------------|
     *                                      |- PUID: nodeId ---------| |- PUID: userId ---------| |- PUID: connectionId ---| |- PUID: transactionId --|   |- PUID: nodeId ---------| |- PUID: adapterId --------| |- PUID: reserved -------| |- PUID: custom ---------|
     *                                      |- 8 Bytes -||- 8 Bytes -| |- 8 Bytes -||- 8 Bytes -| |- 8 Bytes -||- 8 Bytes -| |- 8 Bytes -||- 8 Bytes -|   |- 8 Bytes -||- 8 Bytes -| |- 8 Bytes -||- 8 Bytes -| |- 8 Bytes -||- 8 Bytes -| |- 8 Bytes -||- 8 Bytes -|
     *                                      most sig         least sig
     */
    private PolyXid( final PUID nodeId, final PUID userId, final PUID connectionId, final PUID transactionId ) {
        this.formatId = 0;

        //
        setField( this.globalTransactionId, NODE_ID_LSB_INDEX, nodeId.getMostSignificantBits(), nodeId.getLeastSignificantBits() );
        setField( this.globalTransactionId, USER_ID_LSB_INDEX, userId.getMostSignificantBits(), userId.getLeastSignificantBits() );
        setField( this.globalTransactionId, CONNECTION_ID_LSB_INDEX, connectionId.getMostSignificantBits(), connectionId.getLeastSignificantBits() );
        setField( this.globalTransactionId, TRANSACTION_ID_LSB_INDEX, transactionId.getMostSignificantBits(), transactionId.getLeastSignificantBits() );

        //
        setField( this.branchQualifier, NODE_ID_LSB_INDEX, nodeId.getMostSignificantBits(), nodeId.getLeastSignificantBits() );
    }


    private PolyXid( final PolyXid globalTransactionId, final PUID nodeId ) {
        this.formatId = 0;

        //
        System.arraycopy( globalTransactionId.globalTransactionId, 0, this.globalTransactionId, 0, Math.min( globalTransactionId.globalTransactionId.length, this.globalTransactionId.length ) );

        //
        setField( this.branchQualifier, NODE_ID_LSB_INDEX, nodeId.getMostSignificantBits(), nodeId.getLeastSignificantBits() );
    }


    /*
     * local transaction
     */
    private PolyXid( final PUID nodeId, final PUID transactionId ) {
        this.formatId = 0;

        //
        // NO (origin) NODE
        // NO USER
        // NO CONNECTION
        setField( this.globalTransactionId, TRANSACTION_ID_LSB_INDEX, transactionId.getMostSignificantBits(), transactionId.getLeastSignificantBits() );

        //
        setField( this.branchQualifier, NODE_ID_LSB_INDEX, nodeId.getMostSignificantBits(), nodeId.getLeastSignificantBits() );
    }


    public PolyXid( final byte[] globalTransactionId, final byte[] branchQualifier ) {
        this.formatId = 0;

        if ( globalTransactionId.length > this.globalTransactionId.length ) {
            throw new IllegalArgumentException( "The GlobalTransactionId is too large. Max size is " + (this.globalTransactionId.length) + " bytes." );
        }
        if ( branchQualifier.length > this.branchQualifier.length ) {
            throw new IllegalArgumentException( "The BranchQualifier is too large. Max size is " + (this.branchQualifier.length) + " bytes." );
        }

        System.arraycopy( globalTransactionId, 0, this.globalTransactionId, 0, Math.min( globalTransactionId.length, this.globalTransactionId.length ) );
        System.arraycopy( branchQualifier, 0, this.branchQualifier, 0, Math.min( branchQualifier.length, this.branchQualifier.length ) );
    }


    /*
     * COPY-CONSTRUCTOR
     */
    public PolyXid( final javax.transaction.xa.Xid xid ) {
        this.formatId = xid.getFormatId();
        System.arraycopy( xid.getGlobalTransactionId(), 0, this.globalTransactionId, 0, Math.min( xid.getGlobalTransactionId().length, this.globalTransactionId.length ) );
        System.arraycopy( xid.getBranchQualifier(), 0, this.branchQualifier, 0, Math.min( xid.getBranchQualifier().length, this.branchQualifier.length ) );
    }


    /**
     * @return If OSI CCR naming is used, then the XID’s formatID element should be set to 0; if some other format is used, then the formatID element should be greater than 0. A value of −1 in formatID means that the XID is null.
     */
    @Override
    public int getFormatId() {
        return this.formatId;
    }


    @Override
    public byte[] getGlobalTransactionId() {
        return this.globalTransactionId.clone();
    }


    @Override
    public byte[] getBranchQualifier() {
        return this.branchQualifier.clone();
    }


    private PUID extractPUID( final byte[] polyXidPart, final int leastSignificantBytesIndex ) {
        long[] field = getField( polyXidPart, leastSignificantBytesIndex );
        return new PUID( field[0], field[1] );
    }


    private long[] getField( final byte[] polyXidPart, final int lsbIndex ) {
        byte[] leastSignificantBytes = new byte[Long.BYTES];
        System.arraycopy( polyXidPart, lsbIndex, leastSignificantBytes, 0, leastSignificantBytes.length );
        byte[] mostSignificantBytes = new byte[Long.BYTES];
        System.arraycopy( polyXidPart, lsbIndex + Long.BYTES, mostSignificantBytes, 0, mostSignificantBytes.length );

        return new long[]{ Utils.bytesToLong( mostSignificantBytes ), Utils.bytesToLong( leastSignificantBytes ) };
    }


    private void setField( final byte[] polyXidPart, final int leastSignificantBytesIndex, final long mostSignificantBytes, final long leastSignificantBytes ) {
        System.arraycopy( Utils.longToBytes( leastSignificantBytes ), 0, polyXidPart, leastSignificantBytesIndex, Long.BYTES );
        System.arraycopy( Utils.longToBytes( mostSignificantBytes ), 0, polyXidPart, leastSignificantBytesIndex + Long.BYTES, Long.BYTES );
    }


    public PUID getNodeId() {
        return extractPUID( this.globalTransactionId, NODE_ID_LSB_INDEX );
    }


    public PUID getConnectionId() {
        return extractPUID( this.globalTransactionId, CONNECTION_ID_LSB_INDEX );
    }


    public PUID getUserId() {
        return extractPUID( this.globalTransactionId, USER_ID_LSB_INDEX );
    }


    public PUID getTransactionId() {
        return extractPUID( this.globalTransactionId, TRANSACTION_ID_LSB_INDEX );
    }


    public PUID getStoreId() {
        return extractPUID( this.branchQualifier, STORE_ID_LSB_INDEX );
    }


    public void setStoreId( final PUID storeId ) {
        setStoreField( storeId.getMostSignificantBits(), storeId.getLeastSignificantBits() );
    }


    public long[] getStoreField() {
        return getField( this.branchQualifier, STORE_ID_LSB_INDEX );
    }


    public void setStoreField( final long mostSignificantBytes, final long leastSignificantBytes ) {
        setField( this.branchQualifier, STORE_ID_LSB_INDEX, mostSignificantBytes, leastSignificantBytes );
    }


    public PUID getCustomId() {
        return extractPUID( this.branchQualifier, CUSTOM_ID_LSB_INDEX );
    }


    public void setCustomId( final PUID customId ) {
        setCustomField( customId.getMostSignificantBits(), customId.getLeastSignificantBits() );
    }


    public long[] getCustomField() {
        return getField( this.branchQualifier, CUSTOM_ID_LSB_INDEX );
    }


    public void setCustomField( final long mostSignificantBytes, final long leastSignificantBytes ) {
        setField( this.branchQualifier, CUSTOM_ID_LSB_INDEX, mostSignificantBytes, leastSignificantBytes );
    }


    public boolean isLocalTransactionIdentifier() {
        // NO (origin) NODE
        // NO USER
        // NO CONNECTION
        return getNodeId().equals( PUID.EMPTY_PUID ) && getUserId().equals( PUID.EMPTY_PUID ) && getConnectionId().equals( PUID.EMPTY_PUID );
    }


    @Override
    public String toString() {
        return "{GID:" + Hex.encodeHexString( this.globalTransactionId ) + ",BID:" + Hex.encodeHexString( this.branchQualifier ) + "}";
    }


    private static final Object[] CLONE_LOCK = new Object[0];


    @Override
    public PolyXid clone() {
        try {
            final PolyXid clone = (PolyXid) super.clone();

            final Field globalTransactionId_Field = PolyXid.class.getDeclaredField( "globalTransactionId" );
            final Field branchQualifier_Field = PolyXid.class.getDeclaredField( "branchQualifier" );

            synchronized ( CLONE_LOCK ) {

                globalTransactionId_Field.setAccessible( true );
                branchQualifier_Field.setAccessible( true );

                globalTransactionId_Field.set( clone, this.globalTransactionId.clone() );
                branchQualifier_Field.set( clone, this.branchQualifier.clone() );

                globalTransactionId_Field.setAccessible( false );
                branchQualifier_Field.setAccessible( false );
            }

            return clone;
        } catch ( CloneNotSupportedException | NoSuchFieldException e ) {
            throw new RuntimeException( "This should not happen.", e );
        } catch ( IllegalAccessException e ) {
            throw new RuntimeException( "Cannot clone the PolyXid.", e );
        }
    }


    public boolean belongsTo( final PolyXid transactionId ) {
        return this.formatId == transactionId.formatId && Arrays.equals( this.globalTransactionId, transactionId.globalTransactionId );
    }

}
