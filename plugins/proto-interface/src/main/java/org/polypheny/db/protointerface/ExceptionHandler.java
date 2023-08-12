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

import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.protobuf.ProtoUtils;
import java.util.Arrays;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.remote.AvaticaRuntimeException;
import org.polypheny.db.protointerface.proto.ErrorDetails;

@Slf4j
public class ExceptionHandler implements ServerInterceptor {

    private static final Metadata.Key<ErrorDetails> ERROR_DETAILS_KEY = ProtoUtils.keyForProto( ErrorDetails.getDefaultInstance() );


    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata metadata,
            ServerCallHandler<ReqT, RespT> nextHandler ) {
        ServerCall.Listener<ReqT> listener = nextHandler.startCall( call, metadata );
        return new ExceptionHandlingServerCallListener<>( listener, call, metadata );
    }


    private class ExceptionHandlingServerCallListener<ReqT, RespT> extends ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT> {

        private final ServerCall<ReqT, RespT> serverCall;
        private final Metadata metadata;


        ExceptionHandlingServerCallListener( ServerCall.Listener<ReqT> listener, ServerCall<ReqT, RespT> serverCall, Metadata metadata ) {
            super( listener );
            this.serverCall = serverCall;
            this.metadata = metadata;
        }


        @Override
        public void onHalfClose() {
            try {
                super.onHalfClose();
            } catch ( PIServiceException e ) {
                handleProtoInterfaceServiceException( e, serverCall, metadata );
                throw e;
            } catch ( AvaticaRuntimeException e ) {
                handleAvaticaRuntimeException( e, serverCall, metadata );
            } catch ( Exception e ) {
                handleGenericExceptions( e, serverCall, metadata );
            }
        }


        private void handleGenericExceptions( Exception exception, ServerCall<ReqT, RespT> serverCall, Metadata metadata ) {
            //serverCall.close(Status.fromThrowable(exception), metadata);
            ErrorDetails.Builder errorDetailsBuilder = ErrorDetails.newBuilder();
            if (exception.getMessage() == null) {
                errorDetailsBuilder.setMessage( "No information provided. Returning stacktrace instead: " + Arrays.toString( exception.getStackTrace() ) );
            } else {
                errorDetailsBuilder.setMessage(exception.getMessage());
            }
            metadata.put( ERROR_DETAILS_KEY, errorDetailsBuilder.build() );
            serverCall.close( Status.INTERNAL.withDescription( exception.getMessage() ), metadata );
        }


        private void handleProtoInterfaceServiceException( PIServiceException exception, ServerCall<ReqT, RespT> serverCall, Metadata metadata ) {
            ErrorDetails errorDetails = exception.getProtoErrorDetails();
            metadata.put( ERROR_DETAILS_KEY, errorDetails );
            serverCall.close( Status.INTERNAL.withDescription( exception.getMessage() ), metadata );
        }


        private void handleAvaticaRuntimeException( AvaticaRuntimeException avaticaRuntimeException, ServerCall<ReqT, RespT> serverCall, Metadata metadata ) {
            //serverCall.close(Status.fromThrowable(avaticaRuntimeException), metadata);
            ErrorDetails.Builder errorDetailsBuilder = ErrorDetails.newBuilder();
            errorDetailsBuilder.setErrorCode( avaticaRuntimeException.getErrorCode() );
            errorDetailsBuilder.setState( avaticaRuntimeException.getSqlState() );
            Optional.ofNullable( avaticaRuntimeException.getErrorMessage() ).ifPresent( errorDetailsBuilder::setMessage );
            metadata.put( ERROR_DETAILS_KEY, errorDetailsBuilder.build() );
            serverCall.close( Status.INTERNAL.withDescription( avaticaRuntimeException.getErrorMessage() ), metadata );
        }

    }

}
