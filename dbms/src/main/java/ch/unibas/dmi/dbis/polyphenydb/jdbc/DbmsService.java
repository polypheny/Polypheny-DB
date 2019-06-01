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

package ch.unibas.dmi.dbis.polyphenydb.jdbc;


import java.sql.SQLException;
import org.apache.calcite.avatica.metrics.MetricsSystem;
import org.apache.calcite.avatica.metrics.noop.NoopMetricsSystem;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.remote.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 */
public class DbmsService implements Service {

    private static final Logger LOGGER = LoggerFactory.getLogger( DbmsService.class );

    private LocalService delegate;
    private RpcMetadataResponse rpcMetaData;


    public DbmsService() throws SQLException {
        this( NoopMetricsSystem.getInstance() );
    }


    public DbmsService( MetricsSystem metrics ) throws SQLException {
        this.delegate = new LocalService( DbmsMeta.instance(), metrics );
    }


    @Override
    public ResultSetResponse apply( CatalogsRequest request ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "apply( CatalogsRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public ResultSetResponse apply( SchemasRequest request ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "apply( SchemasRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public ResultSetResponse apply( TablesRequest request ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "apply( TablesRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public ResultSetResponse apply( TableTypesRequest request ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "apply( TableTypesRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public ResultSetResponse apply( TypeInfoRequest request ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "apply( TypeInfoRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public ResultSetResponse apply( ColumnsRequest request ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "apply( ColumnsRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public PrepareResponse apply( PrepareRequest request ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "apply( PrepareRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public ExecuteResponse apply( ExecuteRequest request ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "apply( ExecuteRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public ExecuteResponse apply( PrepareAndExecuteRequest request ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "apply( PrepareAndExecuteRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public SyncResultsResponse apply( SyncResultsRequest request ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "apply( SyncResultsRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public FetchResponse apply( FetchRequest request ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "apply( FetchRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public CreateStatementResponse apply( CreateStatementRequest request ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "apply( CreateStatementRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public CloseStatementResponse apply( CloseStatementRequest request ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "apply( CloseStatementRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public OpenConnectionResponse apply( OpenConnectionRequest request ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "apply( OpenConnectionRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public CloseConnectionResponse apply( CloseConnectionRequest request ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "apply( CloseConnectionRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public ConnectionSyncResponse apply( ConnectionSyncRequest request ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "apply( ConnectionSyncRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public DatabasePropertyResponse apply( DatabasePropertyRequest request ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "apply( DatabasePropertyRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public CommitResponse apply( CommitRequest request ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "apply( CommitRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public RollbackResponse apply( RollbackRequest request ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "apply( RollbackRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public ExecuteBatchResponse apply( PrepareAndExecuteBatchRequest request ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "apply( PrepareAndExecuteBatchRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public ExecuteBatchResponse apply( ExecuteBatchRequest request ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "apply( ExecuteBatchRequest {} )", request );
        }
        return delegate.apply( request );
    }


    @Override
    public void setRpcMetadata( RpcMetadataResponse metadata ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "setRpcMetadata( RpcMetadataResponse " + metadata + " )" );
        }
        this.rpcMetaData = metadata;
        delegate.setRpcMetadata( metadata );
    }
}
