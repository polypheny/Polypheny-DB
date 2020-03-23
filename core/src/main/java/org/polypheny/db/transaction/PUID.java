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


import java.io.Serializable;
import java.util.UUID;
import lombok.EqualsAndHashCode;


/**
 * Polypheny-DB Unique Identifier (PUID)
 */
@EqualsAndHashCode(doNotUseGetters = true)
public class PUID implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final PUID EMPTY_PUID = new PUID( 0L, 0L );


    public static PUID randomPUID( final Type type ) {
        switch ( type ) {
            case OTHER:
                break;
            case RANDOM:
                break;
            case NODE:
                return new NodeId();
            case USER:
                return new UserId();
            case CONNECTION:
                return new ConnectionId();
            case STATEMENT:
                return new StatementId();
            case PREPARED_STATEMENT:
                break;
            case RESULT_SET:
                break;
            case TRANSACTION:
                break;
        }
        return new PUID( type );
    }


    public static PUID fromString( final String puid ) {
        return new PUID( puid );
    }


    private final UUID puid;


    public PUID( long mostSignificantBits, long leastSignificantBits ) {
        this.puid = new UUID( mostSignificantBits, leastSignificantBits );
    }


    private PUID( final Type type ) {
        this.puid = UUID.randomUUID();
    }


    private PUID( final String puid ) {
        this.puid = UUID.fromString( puid );
    }


    public PUID( byte[] bytes ) {
        this.puid = UUID.nameUUIDFromBytes( bytes );
    }


    /**
     * Returns the least significant 64 bits of this UUID's 128 bit value.
     *
     * @return The least significant 64 bits of this UUID's 128 bit value
     */
    public long getLeastSignificantBits() {
        return puid.getLeastSignificantBits();
    }


    /**
     * Returns the most significant 64 bits of this UUID's 128 bit value.
     *
     * @return The most significant 64 bits of this UUID's 128 bit value
     */
    public long getMostSignificantBits() {
        return puid.getMostSignificantBits();
    }


    @Override
    public String toString() {
        return puid.toString();
    }


    public enum Type {
        OTHER( (byte) 0x00 ),
        RANDOM( (byte) 0xff ),
        NODE( (byte) 0x01 ),
        USER( (byte) 0x05 ),
        CONNECTION( (byte) 0x0a ),
        STATEMENT( (byte) 0x10 ),
        PREPARED_STATEMENT( (byte) 0x11 ),
        RESULT_SET( (byte) 0x20 ),
        TRANSACTION( (byte) 0x30 ),
        //
        ;


        public static Type extractType( final PUID puid ) {
            return OTHER;
        }


        private final byte indicator;


        Type( final byte indicator ) {
            this.indicator = indicator;
        }


        public byte getIndicator() {
            return indicator;
        }
    }


    public static class NodeId extends PUID {

        private NodeId() {
            super( Type.NODE );
        }


        private NodeId( final String nodeId ) {
            super( nodeId );
        }


        public static NodeId fromString( final String nodeId ) {
            return new NodeId( nodeId );
        }
    }


    public static class UserId extends PUID {

        private UserId() {
            super( Type.USER );
        }


        private UserId( final byte[] userName ) {
            super( userName );
        }


        public static UserId fromString( final String userName ) {
            return new UserId( userName.getBytes() );
        }
    }


    public static class ConnectionId extends PUID {

        private ConnectionId() {
            super( Type.CONNECTION );
        }


        private ConnectionId( final String connectionId ) {
            super( connectionId );
        }


        public static ConnectionId fromString( final String connectionId ) {
            return new ConnectionId( connectionId );
        }
    }


    public static class StatementId extends PUID {

        private StatementId() {
            super( Type.STATEMENT );
        }


        private StatementId( final String statementId ) {
            super( statementId );
        }


        private StatementId( final long statementId ) {
            super( 0L, statementId );
        }


        public static StatementId fromString( final String statementId ) {
            return new StatementId( statementId );
        }


        public static StatementId fromInt( final int statementId ) {
            return new StatementId( statementId );
        }
    }
}
