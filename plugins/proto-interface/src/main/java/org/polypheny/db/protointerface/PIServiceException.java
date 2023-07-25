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
import lombok.Getter;
import org.polypheny.db.protointerface.proto.ErrorDetails;

public class PIServiceException extends RuntimeException {

    @Getter
    private String state;
    @Getter
    private int errorCode;


    public PIServiceException( String reason, String state, int errorCode ) {
        super( reason );
        this.state = state;
        this.errorCode = errorCode;
    }


    public PIServiceException( String reason, String state ) {
        super( reason );
        this.state = state;
    }


    public PIServiceException( SQLException sqlException ) {
        super( sqlException.getMessage(),
                sqlException.getCause()
        );
        this.state = sqlException.getSQLState();
        this.errorCode = sqlException.getErrorCode();
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
        super( reason, cause );
        this.state = state;
    }


    public PIServiceException( String reason, String state, int errorCode, Throwable cause ) {
        super( reason, cause );
        this.state = state;
        this.errorCode = errorCode;
    }


    public PIServiceException( ErrorDetails errorDetails ) {
        super( errorDetails.hasMessage() ? errorDetails.getMessage() : null );
        this.state = errorDetails.hasState() ? errorDetails.getState() : null;
        this.errorCode = errorDetails.hasErrorCode() ? errorDetails.getErrorCode() : 0;
    }


    public ErrorDetails getProtoErrorDetails() {
        ErrorDetails.Builder errorDetailsBuilder = ErrorDetails.newBuilder();
        errorDetailsBuilder.setErrorCode( getErrorCode() );
        Optional.ofNullable( getState() ).ifPresent( errorDetailsBuilder::setState );
        Optional.ofNullable( getMessage() ).ifPresent( errorDetailsBuilder::setMessage );
        return errorDetailsBuilder.build();
    }

}
