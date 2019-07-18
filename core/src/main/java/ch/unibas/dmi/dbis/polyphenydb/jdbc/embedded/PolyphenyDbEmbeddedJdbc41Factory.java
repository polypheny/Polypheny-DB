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

package ch.unibas.dmi.dbis.polyphenydb.jdbc.embedded;


import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbFactory;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbPrepare.PolyphenyDbSignature;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbResultSet;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbSchema;
import java.io.InputStream;
import java.io.Reader;
import java.sql.NClob;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Properties;
import java.util.TimeZone;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaDatabaseMetaData;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.AvaticaPreparedStatement;
import org.apache.calcite.avatica.AvaticaResultSetMetaData;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.UnregisteredDriver;


/**
 * Implementation of {@link org.apache.calcite.avatica.AvaticaFactory} for Polypheny-DB and JDBC 4.1 (corresponds to JDK 1.7).
 */
@SuppressWarnings("UnusedDeclaration")
public class PolyphenyDbEmbeddedJdbc41Factory extends PolyphenyDbFactory {

    /**
     * Creates a factory for JDBC version 4.1.
     */
    public PolyphenyDbEmbeddedJdbc41Factory() {
        this( 4, 1 );
    }


    /**
     * Creates a JDBC factory with given major/minor version number.
     */
    protected PolyphenyDbEmbeddedJdbc41Factory( int major, int minor ) {
        super( major, minor );
    }


    public PolyphenyDbJdbc41Connection newConnection( UnregisteredDriver driver, AvaticaFactory factory, String url, Properties info, PolyphenyDbSchema rootSchema, JavaTypeFactory typeFactory ) {
        return new PolyphenyDbJdbc41Connection( (EmbeddedDriver) driver, factory, url, info, rootSchema, typeFactory );
    }


    public PolyphenyDbJdbc41DatabaseMetaData newDatabaseMetaData( AvaticaConnection connection ) {
        return new PolyphenyDbJdbc41DatabaseMetaData( (PolyphenyDbEmbeddedConnectionImpl) connection );
    }


    public PolyphenyDbJdbc41Statement newStatement( AvaticaConnection connection, Meta.StatementHandle h, int resultSetType, int resultSetConcurrency, int resultSetHoldability ) {
        return new PolyphenyDbJdbc41Statement( (PolyphenyDbEmbeddedConnectionImpl) connection, h, resultSetType, resultSetConcurrency, resultSetHoldability );
    }


    public AvaticaPreparedStatement newPreparedStatement( AvaticaConnection connection, Meta.StatementHandle h, Meta.Signature signature, int resultSetType, int resultSetConcurrency, int resultSetHoldability ) throws SQLException {
        return new PolyphenyDbJdbc41PreparedStatement( (PolyphenyDbEmbeddedConnectionImpl) connection, h, (PolyphenyDbSignature) signature, resultSetType, resultSetConcurrency, resultSetHoldability );
    }


    public PolyphenyDbResultSet newResultSet( AvaticaStatement statement, QueryState state, Meta.Signature signature, TimeZone timeZone, Meta.Frame firstFrame ) throws SQLException {
        final ResultSetMetaData metaData = newResultSetMetaData( statement, signature );
        final PolyphenyDbSignature polyphenyDbSignature = (PolyphenyDbSignature) signature;
        return new PolyphenyDbResultSet( statement, polyphenyDbSignature, metaData, timeZone, firstFrame );
    }


    public ResultSetMetaData newResultSetMetaData( AvaticaStatement statement, Meta.Signature signature ) {
        return new AvaticaResultSetMetaData( statement, null, signature );
    }


    /**
     * Implementation of connection for JDBC 4.1.
     */
    private static class PolyphenyDbJdbc41Connection extends PolyphenyDbEmbeddedConnectionImpl {

        PolyphenyDbJdbc41Connection( EmbeddedDriver embeddedDriver, AvaticaFactory factory, String url, Properties info, PolyphenyDbSchema rootSchema, JavaTypeFactory typeFactory ) {
            super( embeddedDriver, factory, url, info, rootSchema, typeFactory );
        }
    }


    /**
     * Implementation of statement for JDBC 4.1.
     */
    private static class PolyphenyDbJdbc41Statement extends PolyphenyDbEmbeddedStatement {

        PolyphenyDbJdbc41Statement( PolyphenyDbEmbeddedConnectionImpl connection, Meta.StatementHandle h, int resultSetType, int resultSetConcurrency, int resultSetHoldability ) {
            super( connection, h, resultSetType, resultSetConcurrency, resultSetHoldability );
        }
    }


    /**
     * Implementation of prepared statement for JDBC 4.1.
     */
    private static class PolyphenyDbJdbc41PreparedStatement extends PolyphenyDbEmbeddedPreparedStatement {

        PolyphenyDbJdbc41PreparedStatement( PolyphenyDbEmbeddedConnectionImpl connection, Meta.StatementHandle h, PolyphenyDbSignature signature, int resultSetType, int resultSetConcurrency, int resultSetHoldability ) throws SQLException {
            super( connection, h, signature, resultSetType, resultSetConcurrency, resultSetHoldability );
        }


        public void setRowId( int parameterIndex, RowId x ) throws SQLException {
            getSite( parameterIndex ).setRowId( x );
        }


        public void setNString( int parameterIndex, String value ) throws SQLException {
            getSite( parameterIndex ).setNString( value );
        }


        public void setNCharacterStream( int parameterIndex, Reader value, long length ) throws SQLException {
            getSite( parameterIndex ).setNCharacterStream( value, length );
        }


        public void setNClob( int parameterIndex, NClob value ) throws SQLException {
            getSite( parameterIndex ).setNClob( value );
        }


        public void setClob( int parameterIndex, Reader reader, long length ) throws SQLException {
            getSite( parameterIndex ).setClob( reader, length );
        }


        public void setBlob( int parameterIndex, InputStream inputStream, long length ) throws SQLException {
            getSite( parameterIndex ).setBlob( inputStream, length );
        }


        public void setNClob( int parameterIndex, Reader reader, long length ) throws SQLException {
            getSite( parameterIndex ).setNClob( reader, length );
        }


        public void setSQLXML( int parameterIndex, SQLXML xmlObject ) throws SQLException {
            getSite( parameterIndex ).setSQLXML( xmlObject );
        }


        public void setAsciiStream( int parameterIndex, InputStream x, long length ) throws SQLException {
            getSite( parameterIndex ).setAsciiStream( x, length );
        }


        public void setBinaryStream( int parameterIndex, InputStream x, long length ) throws SQLException {
            getSite( parameterIndex ).setBinaryStream( x, length );
        }


        public void setCharacterStream( int parameterIndex, Reader reader, long length ) throws SQLException {
            getSite( parameterIndex ).setCharacterStream( reader, length );
        }


        public void setAsciiStream( int parameterIndex, InputStream x ) throws SQLException {
            getSite( parameterIndex ).setAsciiStream( x );
        }


        public void setBinaryStream( int parameterIndex, InputStream x ) throws SQLException {
            getSite( parameterIndex ).setBinaryStream( x );
        }


        public void setCharacterStream( int parameterIndex, Reader reader ) throws SQLException {
            getSite( parameterIndex ).setCharacterStream( reader );
        }


        public void setNCharacterStream( int parameterIndex, Reader value ) throws SQLException {
            getSite( parameterIndex ).setNCharacterStream( value );
        }


        public void setClob( int parameterIndex, Reader reader ) throws SQLException {
            getSite( parameterIndex ).setClob( reader );
        }


        public void setBlob( int parameterIndex, InputStream inputStream ) throws SQLException {
            getSite( parameterIndex ).setBlob( inputStream );
        }


        public void setNClob( int parameterIndex, Reader reader ) throws SQLException {
            getSite( parameterIndex ).setNClob( reader );
        }
    }


    /**
     * Implementation of database metadata for JDBC 4.1.
     */
    private static class PolyphenyDbJdbc41DatabaseMetaData extends AvaticaDatabaseMetaData {

        PolyphenyDbJdbc41DatabaseMetaData( PolyphenyDbEmbeddedConnectionImpl connection ) {
            super( connection );
        }
    }
}

