/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.postgresql;

import lombok.extern.slf4j.Slf4j;


/**
 * Contains information for what will be sent to the client, and also methods to handle the information
 */
@Slf4j
public class PGInterfaceMessage {

    private static final char delimiter = '§'; //for the subparts
    private final boolean defaultLength;
    private PGInterfaceHeaders header;
    private String msgBody;
    private int length; //default is 4, if a different length is mentioned in protocol, this is given
    private PGInterfaceErrorHandler errorHandler;


    /**
     * Creates a PG-Message. It contains all relevant information to send a message to the client
     *
     * @param header        What header should be sent (depends on message-type)
     * @param msgBody       The message itself
     * @param length        The length of the message (the length itself is included)
     * @param defaultLength The length of the length sent (which is included). Length is 4
     */
    public PGInterfaceMessage( PGInterfaceHeaders header, String msgBody, int length, boolean defaultLength ) {
        this.header = header;
        this.msgBody = msgBody;
        this.length = length;
        this.defaultLength = defaultLength;
    }

    /**
     * Get what delimiter is currently set
     *
     * @return the current delimiter as string
     */
    public static String getDelimiter() {
        String del = String.valueOf( delimiter );
        return del;
    }

    /**
     * get the header of the message
     *
     * @return PGInterfaceHeader of the message
     */
    public PGInterfaceHeaders getHeader() {
        return this.header;
    }

    public void setHeader( PGInterfaceHeaders header ) {
        this.header = header;
    }

    /**
     * returns the PGInterfaceHeader of the Message as a char
     *
     * @return PGInterfaceHeader as a char
     */
    public char getHeaderChar() {

        //if header is a single character
        if ( header != PGInterfaceHeaders.ONE && header != PGInterfaceHeaders.TWO && header != PGInterfaceHeaders.THREE ) {
            String headerString = header.toString();
            return headerString.charAt( 0 );
        }
        //if header is a number
        //TODO(FF): make a nicer version of this... if you cast headerInt to char directly it returns '\u0001' and not '1'
        else {
            int headerInt = getHeaderInt();
            if ( headerInt == 1 ) {
                return '1';
            } else if ( headerInt == 2 ) {
                return '2';
            } else if ( headerInt == 3 ) {
                return '3';
            }
        }
        //TODO(FF): does it continue to send things to the client after the error message?
        errorHandler.sendSimpleErrorMessage( "PGInterface>PGInterfaceMessage>getHeaderChar: This should never be reached." );

        return 0;
    }

    /**
     * Changes the three headers that are a number and not a letter into a number (they are safed as a string in the PGInterfaceHeaders)
     *
     * @return 1, 2 or 3 - headers which are numbers
     */
    public int getHeaderInt() {
        String headerString = header.toString();
        if ( headerString.equals( "ONE" ) ) {
            return 1;
        } else if ( headerString.equals( "TWO" ) ) {
            return 2;
        } else if ( headerString.equals( "THREE" ) ) {
            return 3;
        }
        //TODO(FF): does it continue to send things to the client after the error message?
        errorHandler.sendSimpleErrorMessage( "PGInterface>PGInterfaceMessage>getHeaderInt: This should never be reached." );
        return 0;
    }

    /**
     * the length that should be set in the message to the client (default is 4)
     *
     * @return length of the message
     */
    public int getLength() {
        return this.length;
    }

    /**
     * set the length of the message to the client (the default is set to 4)
     *
     * @param length length you want to set as the message length
     */
    public void setLength( int length ) {
        this.length = length;
    }

    /**
     * if the message has the default length (4)
     *
     * @return whether the message has the default length
     */
    public boolean isDefaultLength() {
        return this.defaultLength;
    }

    /**
     * The content of the message that will be sent to the client, can contain several "sub" messages (message fields) which are seperated by the delimiter
     *
     * @return message to the client
     */
    public String getMsgBody() {
        return msgBody;
    }

    /**
     * Set the content of the message that will be sent to the client, can contain several "sub" messages (message fields) which are seperated by the delimiter
     *
     * @param msgBody message to the client
     */
    public void setMsgBody( String msgBody ) {
        this.msgBody = msgBody;
    }

    /**
     * Gets the different sub-parts of a message, which are seperated by the delimiter
     *
     * @param part the index of the requested part(s), starting at 0
     * @return a string array with each requested part
     */
    public String[] getMsgPart( int[] part ) {
        String[] subStrings = msgBody.split( getDelimiter() );
        String[] result = new String[part.length];

        for ( int i = 0; i < (part.length); i++ ) {
            result[i] = subStrings[i];
        }
        return result;
    }

}
