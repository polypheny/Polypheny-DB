/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
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
 */

package ch.unibas.dmi.dbis.polyphenydb.sql;


import ch.unibas.dmi.dbis.polyphenydb.sql.dialect.AccessSqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.dialect.AnsiSqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.dialect.BigQuerySqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.dialect.Db2SqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.dialect.DerbySqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.dialect.FirebirdSqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.dialect.H2SqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.dialect.HiveSqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.dialect.HsqldbSqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.dialect.InfobrightSqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.dialect.InformixSqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.dialect.IngresSqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.dialect.InterbaseSqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.dialect.JethroDataSqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.dialect.LucidDbSqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.dialect.MssqlSqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.dialect.MysqlSqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.dialect.NeoviewSqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.dialect.NetezzaSqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.dialect.OracleSqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.dialect.ParaccelSqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.dialect.PhoenixSqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.dialect.PolyphenyDbSqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.dialect.PostgresqlSqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.dialect.RedshiftSqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.dialect.SybaseSqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.dialect.TeradataSqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.dialect.VerticaSqlDialect;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The default implementation of a <code>SqlDialectFactory</code>.
 */
public class SqlDialectFactoryImpl implements SqlDialectFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger( SqlDialectFactoryImpl.class );

    public static final SqlDialectFactoryImpl INSTANCE = new SqlDialectFactoryImpl();

    private final JethroDataSqlDialect.JethroInfoCache jethroCache = JethroDataSqlDialect.createCache();


    public SqlDialect create( DatabaseMetaData databaseMetaData ) {
        String databaseProductName;
        int databaseMajorVersion;
        int databaseMinorVersion;
        String databaseVersion;
        try {
            databaseProductName = databaseMetaData.getDatabaseProductName();
            databaseMajorVersion = databaseMetaData.getDatabaseMajorVersion();
            databaseMinorVersion = databaseMetaData.getDatabaseMinorVersion();
            databaseVersion = databaseMetaData.getDatabaseProductVersion();
        } catch ( SQLException e ) {
            throw new RuntimeException( "while detecting database product", e );
        }
        final String upperProductName = databaseProductName.toUpperCase( Locale.ROOT ).trim();
        final String quoteString = getIdentifierQuoteString( databaseMetaData );
        final NullCollation nullCollation = getNullCollation( databaseMetaData );
        final SqlDialect.Context c = SqlDialect.EMPTY_CONTEXT
                .withDatabaseProductName( databaseProductName )
                .withDatabaseMajorVersion( databaseMajorVersion )
                .withDatabaseMinorVersion( databaseMinorVersion )
                .withDatabaseVersion( databaseVersion )
                .withIdentifierQuoteString( quoteString )
                .withNullCollation( nullCollation );
        switch ( upperProductName ) {
            case "ACCESS":
                return new AccessSqlDialect( c );
            case "APACHE DERBY":
                return new DerbySqlDialect( c );
            case "DBMS:CLOUDSCAPE":
                return new DerbySqlDialect( c );
            case "HIVE":
                return new HiveSqlDialect( c );
            case "INGRES":
                return new IngresSqlDialect( c );
            case "INTERBASE":
                return new InterbaseSqlDialect( c );
            case "JETHRODATA":
                return new JethroDataSqlDialect( c.withJethroInfo( jethroCache.get( databaseMetaData ) ) );
            case "LUCIDDB":
                return new LucidDbSqlDialect( c );
            case "ORACLE":
                return new OracleSqlDialect( c );
            case "PHOENIX":
                return new PhoenixSqlDialect( c );
            case "MYSQL (INFOBRIGHT)":
                return new InfobrightSqlDialect( c );
            case "MYSQL":
                return new MysqlSqlDialect( c );
            case "REDSHIFT":
                return new RedshiftSqlDialect( c );
        }
        // Now the fuzzy matches.
        if ( databaseProductName.startsWith( "DB2" ) ) {
            return new Db2SqlDialect( c );
        } else if ( upperProductName.contains( "FIREBIRD" ) ) {
            return new FirebirdSqlDialect( c );
        } else if ( databaseProductName.startsWith( "Informix" ) ) {
            return new InformixSqlDialect( c );
        } else if ( upperProductName.contains( "NETEZZA" ) ) {
            return new NetezzaSqlDialect( c );
        } else if ( upperProductName.contains( "PARACCEL" ) ) {
            return new ParaccelSqlDialect( c );
        } else if ( databaseProductName.startsWith( "HP Neoview" ) ) {
            return new NeoviewSqlDialect( c );
        } else if ( upperProductName.contains( "POSTGRE" ) ) {
            return new PostgresqlSqlDialect( c );
        } else if ( upperProductName.contains( "SQL SERVER" ) ) {
            return new MssqlSqlDialect( c );
        } else if ( upperProductName.contains( "SYBASE" ) ) {
            return new SybaseSqlDialect( c );
        } else if ( upperProductName.contains( "TERADATA" ) ) {
            return new TeradataSqlDialect( c );
        } else if ( upperProductName.contains( "HSQL" ) ) {
            return new HsqldbSqlDialect( c );
        } else if ( upperProductName.contains( "H2" ) ) {
            return new H2SqlDialect( c );
        } else if ( upperProductName.contains( "VERTICA" ) ) {
            return new VerticaSqlDialect( c );
        } else {
            return new AnsiSqlDialect( c );
        }
    }


    private NullCollation getNullCollation( DatabaseMetaData databaseMetaData ) {
        try {
            if ( databaseMetaData.nullsAreSortedAtEnd() ) {
                return NullCollation.LAST;
            } else if ( databaseMetaData.nullsAreSortedAtStart() ) {
                return NullCollation.FIRST;
            } else if ( databaseMetaData.nullsAreSortedLow() ) {
                return NullCollation.LOW;
            } else if ( databaseMetaData.nullsAreSortedHigh() ) {
                return NullCollation.HIGH;
            } else {
                throw new IllegalArgumentException( "cannot deduce null collation" );
            }
        } catch ( SQLException e ) {
            throw new IllegalArgumentException( "cannot deduce null collation", e );
        }
    }


    private String getIdentifierQuoteString( DatabaseMetaData databaseMetaData ) {
        try {
            return databaseMetaData.getIdentifierQuoteString();
        } catch ( SQLException e ) {
            throw new IllegalArgumentException( "cannot deduce identifier quote string", e );
        }
    }


    /**
     * Returns a basic dialect for a given product, or null if none is known.
     */
    static SqlDialect simple( SqlDialect.DatabaseProduct databaseProduct ) {
        switch ( databaseProduct ) {
            case ACCESS:
                return AccessSqlDialect.DEFAULT;
            case BIG_QUERY:
                return BigQuerySqlDialect.DEFAULT;
            case POLYPHENYDB:
                return PolyphenyDbSqlDialect.DEFAULT;
            case DB2:
                return Db2SqlDialect.DEFAULT;
            case DERBY:
                return DerbySqlDialect.DEFAULT;
            case FIREBIRD:
                return FirebirdSqlDialect.DEFAULT;
            case H2:
                return H2SqlDialect.DEFAULT;
            case HIVE:
                return HiveSqlDialect.DEFAULT;
            case HSQLDB:
                return HsqldbSqlDialect.DEFAULT;
            case INFOBRIGHT:
                return InfobrightSqlDialect.DEFAULT;
            case INFORMIX:
                return InformixSqlDialect.DEFAULT;
            case INGRES:
                return IngresSqlDialect.DEFAULT;
            case INTERBASE:
                return InterbaseSqlDialect.DEFAULT;
            case JETHRO:
                throw new RuntimeException( "Jethro does not support simple creation" );
            case LUCIDDB:
                return LucidDbSqlDialect.DEFAULT;
            case MSSQL:
                return MssqlSqlDialect.DEFAULT;
            case MYSQL:
                return MysqlSqlDialect.DEFAULT;
            case NEOVIEW:
                return NeoviewSqlDialect.DEFAULT;
            case NETEZZA:
                return NetezzaSqlDialect.DEFAULT;
            case ORACLE:
                return OracleSqlDialect.DEFAULT;
            case PARACCEL:
                return ParaccelSqlDialect.DEFAULT;
            case PHOENIX:
                return PhoenixSqlDialect.DEFAULT;
            case POSTGRESQL:
                return PostgresqlSqlDialect.DEFAULT;
            case REDSHIFT:
                return RedshiftSqlDialect.DEFAULT;
            case SYBASE:
                return SybaseSqlDialect.DEFAULT;
            case TERADATA:
                return TeradataSqlDialect.DEFAULT;
            case VERTICA:
                return VerticaSqlDialect.DEFAULT;
            case SQLSTREAM:
            case UNKNOWN:
            default:
                return null;
        }
    }

}
