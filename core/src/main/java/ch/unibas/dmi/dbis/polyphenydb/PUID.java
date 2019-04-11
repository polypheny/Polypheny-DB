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


        private UserId( final String userId ) {
            super( userId );
        }


        public static UserId fromString( final String userId ) {
            return new UserId( userId );
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
