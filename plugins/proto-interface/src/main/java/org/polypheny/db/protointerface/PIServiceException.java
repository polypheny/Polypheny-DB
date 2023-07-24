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

package org.polypheny.db.protointerface;

import java.sql.SQLException;
import java.util.Optional;
import org.polypheny.db.protointerface.proto.ErrorDetails;

public class PIServiceException extends SQLException {

    public PIServiceException( String reason, String state, int errorCode ) {
        super( reason, state, errorCode );
    }


    public PIServiceException( String reason, String state ) {
        super( reason, state );

    }


    public PIServiceException( String reason ) {
        super( reason );
    }


    public PIServiceException() {
        super();
    }


    public PIServiceException( Throwable cause ) {
        super( cause );
    }


    public PIServiceException( String reason, Throwable cause ) {
        super( reason, cause );
    }


    public PIServiceException( String reason, String state, Throwable cause ) {
        super( reason, state, cause );
    }


    public PIServiceException( String reason, String state, int errorCode, Throwable cause ) {
        super( reason, state, errorCode, cause );
    }


    public PIServiceException( ErrorDetails errorDetails ) {
        super(
                errorDetails.hasMessage() ? errorDetails.getMessage() : null,
                errorDetails.hasState() ? errorDetails.getState() : null,
                errorDetails.hasErrorCode() ? errorDetails.getErrorCode() : 0
        );
    }


    public ErrorDetails getProtoErrorDetails() {
        ErrorDetails.Builder errorDetailsBuilder = ErrorDetails.newBuilder();
        errorDetailsBuilder.setErrorCode( getErrorCode() );
        Optional.ofNullable( getSQLState() ).ifPresent( errorDetailsBuilder::setState );
        Optional.ofNullable( getMessage() ).ifPresent( errorDetailsBuilder::setMessage );
        return errorDetailsBuilder.build();
    }
}
