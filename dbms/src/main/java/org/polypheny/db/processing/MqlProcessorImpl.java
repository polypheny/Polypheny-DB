/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.processing;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.jdbc.PolyphenyDbSignature;
import org.polypheny.db.mql.MqlNode;
import org.polypheny.db.mql.parser.MqlParseException;
import org.polypheny.db.mql.parser.MqlParser;
import org.polypheny.db.mql.parser.MqlParser.MqlParserConfig;
import org.polypheny.db.plan.RelOptTable.ViewExpander;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.SourceStringReader;

@Slf4j
public class MqlProcessorImpl implements MqlProcessor, ViewExpander {

    private static final MqlParserConfig parserConfig;


    static {
        MqlParser.ConfigBuilder configConfigBuilder = MqlParser.configBuilder();
        parserConfig = configConfigBuilder.build();
    }


    @Override
    public RelRoot expandView( RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath ) {
        return null;
    }


    @Override
    public MqlNode parse( String mql ) {
        final StopWatch stopWatch = new StopWatch();
        if ( log.isDebugEnabled() ) {
            log.debug( "Parsing PolyMQL statement ..." );
        }
        stopWatch.start();
        MqlNode parsed;
        log.debug( "MQL: {}", mql );

        try {
            final MqlParser parser = MqlParser.create( new SourceStringReader( mql ), parserConfig );
            parsed = parser.parseStmt();
        } catch ( MqlParseException e ) {
            log.error( "Caught exception", e );
            throw new RuntimeException( e );
        }
        stopWatch.stop();
        if ( log.isTraceEnabled() ) {
            log.trace( "Parsed query: [{}]", parsed );
        }
        if ( log.isDebugEnabled() ) {
            log.debug( "Parsing PolyMQL statement ... done. [{}]", stopWatch );
        }
        return parsed;
    }


    @Override
    public Pair<MqlNode, RelDataType> validate( Transaction transaction, MqlNode parsed, boolean addDefaultValues ) {
        return null;
    }


    @Override
    public RelRoot translate( Statement statement, MqlNode sql ) {
        return null;
    }


    @Override
    public PolyphenyDbSignature<?> prepareDdl( Statement statement, MqlNode parsed ) {
        return null;
    }


    @Override
    public RelDataType getParameterRowType( MqlNode left ) {
        return null;
    }

}
