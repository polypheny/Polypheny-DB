/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.sql.language;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.NonNull;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.algebra.constant.NullCollation;
import org.polypheny.db.sql.language.dialect.AccessSqlDialect;
import org.polypheny.db.sql.language.dialect.BigQuerySqlDialect;
import org.polypheny.db.sql.language.dialect.Db2SqlDialect;
import org.polypheny.db.sql.language.dialect.DerbySqlDialect;
import org.polypheny.db.sql.language.dialect.FirebirdSqlDialect;
import org.polypheny.db.sql.language.dialect.H2SqlDialect;
import org.polypheny.db.sql.language.dialect.HiveSqlDialect;
import org.polypheny.db.sql.language.dialect.InfobrightSqlDialect;
import org.polypheny.db.sql.language.dialect.InformixSqlDialect;
import org.polypheny.db.sql.language.dialect.IngresSqlDialect;
import org.polypheny.db.sql.language.dialect.InterbaseSqlDialect;
import org.polypheny.db.sql.language.dialect.LucidDbSqlDialect;
import org.polypheny.db.sql.language.dialect.MssqlSqlDialect;
import org.polypheny.db.sql.language.dialect.MysqlSqlDialect;
import org.polypheny.db.sql.language.dialect.NeoviewSqlDialect;
import org.polypheny.db.sql.language.dialect.NetezzaSqlDialect;
import org.polypheny.db.sql.language.dialect.OracleSqlDialect;
import org.polypheny.db.sql.language.dialect.ParaccelSqlDialect;
import org.polypheny.db.sql.language.dialect.PhoenixSqlDialect;
import org.polypheny.db.sql.language.dialect.PolyphenyDbSqlDialect;
import org.polypheny.db.sql.language.dialect.RedshiftSqlDialect;
import org.polypheny.db.sql.language.dialect.SybaseSqlDialect;
import org.polypheny.db.sql.language.dialect.TeradataSqlDialect;
import org.polypheny.db.sql.language.dialect.VerticaSqlDialect;

public class SqlDialectRegistry {

    private static final Map<@NonNull String, @NonNull SqlDialect> DIALECT_REGISTRY = new ConcurrentHashMap<>();


    static {
        registerDialect( "Access", AccessSqlDialect.DEFAULT );
        registerDialect( "Google BigQuery", BigQuerySqlDialect.DEFAULT );
        registerDialect( "Polypheny-DB", PolyphenyDbSqlDialect.DEFAULT );
        registerDialect( "Microsoft SQL Server", MssqlSqlDialect.DEFAULT );
        registerDialect( "MySQL", MysqlSqlDialect.DEFAULT );
        registerDialect( "Oracle", OracleSqlDialect.DEFAULT );
        registerDialect( "Apache Derby", DerbySqlDialect.DEFAULT );
        registerDialect( "IBM DB2", Db2SqlDialect.DEFAULT );
        registerDialect( "Firebird", FirebirdSqlDialect.DEFAULT );
        registerDialect( "H2", H2SqlDialect.DEFAULT );
        registerDialect( "Apache Hive", HiveSqlDialect.DEFAULT );
        registerDialect( "Informix", InformixSqlDialect.DEFAULT );
        registerDialect( "Ingres", IngresSqlDialect.DEFAULT );
        ///registerDialect("JethroData", null); // Jethro does not support simple creation
        registerDialect( "LucidDB", LucidDbSqlDialect.DEFAULT );
        registerDialect( "Interbase", InterbaseSqlDialect.DEFAULT );
        registerDialect( "Phoenix", PhoenixSqlDialect.DEFAULT );
        registerDialect( "Netezza", NetezzaSqlDialect.DEFAULT );
        registerDialect( "Infobright", InfobrightSqlDialect.DEFAULT );
        registerDialect( "Neoview", NeoviewSqlDialect.DEFAULT );
        registerDialect( "Sybase", SybaseSqlDialect.DEFAULT );
        registerDialect( "Teradata", TeradataSqlDialect.DEFAULT );
        registerDialect( "Vertica", VerticaSqlDialect.DEFAULT );
        //registerDialect("SQLstream", null); // Matching DEFAULT not provided
        registerDialect( "Paraccel", ParaccelSqlDialect.DEFAULT );
        registerDialect( "Redshift", RedshiftSqlDialect.DEFAULT );
    }


    public static void registerDialect( @NonNull String name, @NonNull SqlDialect dialect ) {
        DIALECT_REGISTRY.put( name.toLowerCase( Locale.ROOT ).trim(), dialect );
    }


    public static Optional<SqlDialect> getDialect( String name ) {
        return Optional.ofNullable( DIALECT_REGISTRY.get( name.toLowerCase().trim() ) );
    }


    public static void unregisterDialect( String name ) {
        DIALECT_REGISTRY.remove( name.toLowerCase().trim() );
    }


    @Getter
    public static class DatabaseProduct {

        @Nullable
        private final SqlDialect dialect;
        @NotNull
        private final String databaseProductName;
        @NotNull
        private final String quoteString;
        @NotNull
        private final NullCollation nullCollation;


        private DatabaseProduct( @NotNull String databaseProductName, @NotNull String quoteString, @NotNull NullCollation nullCollation, @Nullable SqlDialect dialect ) {
            this.databaseProductName = databaseProductName;
            this.quoteString = quoteString;
            this.nullCollation = nullCollation;

            this.dialect = dialect;
        }


    }

}
