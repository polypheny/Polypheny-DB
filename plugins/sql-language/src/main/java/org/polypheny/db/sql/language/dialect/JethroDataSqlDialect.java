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

package org.polypheny.db.sql.language.dialect;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.sql.language.SqlDialect;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.type.PolyType;


/**
 * A <code>SqlDialect</code> implementation for the JethroData database.
 */
@Slf4j
public class JethroDataSqlDialect extends SqlDialect {

    private final JethroInfo info;


    /**
     * Creates a JethroDataSqlDialect.
     */
    public JethroDataSqlDialect( Context context ) {
        super( context );
        this.info = context.jethroInfo();
    }


    @Override
    public boolean supportsCharSet() {
        return false;
    }


    @Override
    public SqlNode emulateNullDirection( SqlNode node, boolean nullsFirst, boolean desc ) {
        return node;
    }


    @Override
    public boolean supportsAggregateFunction( Kind kind ) {
        switch ( kind ) {
            case COUNT:
            case SUM:
            case AVG:
            case MIN:
            case MAX:
            case STDDEV_POP:
            case STDDEV_SAMP:
            case VAR_POP:
            case VAR_SAMP:
                return true;
            default:
                break;
        }
        return false;
    }


    @Override
    public boolean supportsNestedAggregations() {
        return false;
    }


    public static JethroInfoCache createCache() {
        return new JethroInfoCacheImpl();
    }


    /**
     * Information about a function supported by Jethro.
     */
    static class JethroSupportedFunction {

        private final List<PolyType> operandTypes;


        JethroSupportedFunction( String name, String operands ) {
            Objects.requireNonNull( name ); // not currently used
            final ImmutableList.Builder<PolyType> b = ImmutableList.builder();
            for ( String strType : operands.split( ":" ) ) {
                b.add( parse( strType ) );
            }
            this.operandTypes = b.build();
        }


        private PolyType parse( String strType ) {
            switch ( strType.toLowerCase( Locale.ROOT ) ) {
                case "bigint":
                case "long":
                    return PolyType.BIGINT;
                case "integer":
                case "int":
                    return PolyType.INTEGER;
                case "double":
                    return PolyType.DOUBLE;
                case "float":
                    return PolyType.FLOAT;
                case "string":
                    return PolyType.VARCHAR;
                case "timestamp":
                    return PolyType.TIMESTAMP;
                default:
                    return PolyType.ANY;
            }
        }


        boolean argumentsMatch( List<AlgDataType> paramTypes ) {
            if ( paramTypes.size() != operandTypes.size() ) {
                return false;
            }
            for ( int i = 0; i < paramTypes.size(); i++ ) {
                if ( paramTypes.get( i ).getPolyType() != operandTypes.get( i ) ) {
                    return false;
                }
            }
            return true;
        }

    }


    /**
     * Stores information about capabilities of Jethro databases.
     */
    public interface JethroInfoCache {

        JethroInfo get( DatabaseMetaData databaseMetaData );

    }


    /**
     * Implementation of {@code JethroInfoCache}.
     */
    private static class JethroInfoCacheImpl implements JethroInfoCache {

        final Map<String, JethroInfo> map = new HashMap<>();


        @Override
        public JethroInfo get( final DatabaseMetaData metaData ) {
            try {
                assert "JethroData".equals( metaData.getDatabaseProductName() );
                String productVersion = metaData.getDatabaseProductVersion();
                synchronized ( JethroInfoCacheImpl.this ) {
                    JethroInfo info = map.get( productVersion );
                    if ( info == null ) {
                        final Connection c = metaData.getConnection();
                        info = makeInfo( c );
                        map.put( productVersion, info );
                    }
                    return info;
                }
            } catch ( Exception e ) {
                log.error( "Failed to create JethroDataDialect", e );
                throw new GenericRuntimeException( "Failed to create JethroDataDialect", e );
            }
        }


        private JethroInfo makeInfo( Connection jethroConnection ) {
            try ( Statement jethroStatement = jethroConnection.createStatement();
                    ResultSet functionsTupleSet = jethroStatement.executeQuery( "show functions extended" ) ) {
                final Multimap<String, JethroSupportedFunction> supportedFunctions = LinkedHashMultimap.create();
                while ( functionsTupleSet.next() ) {
                    String functionName = functionsTupleSet.getString( 1 );
                    String operandsType = functionsTupleSet.getString( 3 );
                    supportedFunctions.put( functionName, new JethroSupportedFunction( functionName, operandsType ) );
                }
                return new JethroInfo( supportedFunctions );
            } catch ( Exception e ) {
                final String msg = "Jethro server failed to execute 'show functions extended'";
                log.error( msg, e );
                throw new GenericRuntimeException( msg + "; make sure your Jethro server is up to date", e );
            }
        }

    }


    /**
     * Information about the capabilities of a Jethro database.
     */
    public static class JethroInfo {

        public static final JethroInfo EMPTY = new JethroInfo( ImmutableSetMultimap.of() );

        private final ImmutableSetMultimap<String, JethroSupportedFunction> supportedFunctions;


        public JethroInfo( Multimap<String, JethroSupportedFunction> supportedFunctions ) {
            this.supportedFunctions = ImmutableSetMultimap.copyOf( supportedFunctions );
        }

    }

}

