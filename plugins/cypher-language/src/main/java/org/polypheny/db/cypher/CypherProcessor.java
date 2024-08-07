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

package org.polypheny.db.cypher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.algebra.AlgDecorrelator;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.ExplainFormat;
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter;
import org.polypheny.db.cypher.parser.CypherParser;
import org.polypheny.db.cypher.parser.CypherParser.CypherParserConfig;
import org.polypheny.db.languages.NodeParseException;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Lock.LockMode;
import org.polypheny.db.transaction.LockManager;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.util.DeadlockException;
import org.polypheny.db.util.Pair;

@Slf4j
public class CypherProcessor extends Processor {

    private static final CypherParserConfig parserConfig;


    static {
        CypherParser.ConfigBuilder configConfigBuilder = CypherParser.configBuilder();
        parserConfig = configConfigBuilder.build();
    }


    @Override
    public List<CypherStatement> parse( String query ) {
        final StopWatch stopWatch = new StopWatch();
        if ( log.isDebugEnabled() ) {
            log.debug( "Parsing PolyMQL statement ..." );
        }
        stopWatch.start();
        List<CypherStatement> parsed;
        if ( log.isDebugEnabled() ) {
            log.debug( "CYPHER: {}", query );
        }

        try {
            final CypherParser parser = CypherParser.create( query, parserConfig );
            parsed = parser.parseStmts();
        } catch ( NodeParseException e ) {
            log.error( "Caught exception", e );
            throw new GenericRuntimeException( e );
        }
        stopWatch.stop();
        if ( log.isTraceEnabled() ) {
            log.trace( "Parsed query: [{}]", parsed );
        }
        if ( log.isDebugEnabled() ) {
            log.debug( "Parsing PolyCypher statement ... done. [{}]", stopWatch );
        }
        return parsed;
    }


    @Override
    public Pair<Node, AlgDataType> validate( Transaction transaction, Node parsed, boolean addDefaultValues ) {
        throw new GenericRuntimeException( "The Cypher implementation does not support validation." );
    }


    @Override
    public AlgRoot translate( Statement statement, ParsedQueryContext context ) {

        final StopWatch stopWatch = new StopWatch();
        if ( log.isDebugEnabled() ) {
            log.debug( "Planning Statement ..." );
        }
        stopWatch.start();

        final AlgBuilder builder = AlgBuilder.create( statement );
        final RexBuilder rexBuilder = new RexBuilder( statement.getTransaction().getTypeFactory() );
        final AlgCluster cluster = AlgCluster.createGraph( statement.getQueryProcessor().getPlanner(), rexBuilder, statement.getDataContext().getSnapshot() );

        final CypherToAlgConverter cypherToAlgConverter = new CypherToAlgConverter( statement, builder, rexBuilder, cluster );

        AlgRoot logicalRoot = cypherToAlgConverter.convert( (CypherNode) context.getQueryNode().orElseThrow(), context, cluster );

        // Decorrelate
        final AlgBuilder algBuilder = AlgBuilder.create( statement );
        logicalRoot = logicalRoot.withAlg( AlgDecorrelator.decorrelateQuery( logicalRoot.alg, algBuilder ) );

        if ( log.isTraceEnabled() ) {
            log.trace( "Logical query plan: [{}]", AlgOptUtil.dumpPlan( "-- Logical Plan", logicalRoot.alg, ExplainFormat.TEXT, ExplainLevel.DIGEST_ATTRIBUTES ) );
        }
        stopWatch.stop();
        if ( log.isDebugEnabled() ) {
            log.debug( "Planning Statement ... done. [{}]", stopWatch );
        }

        return logicalRoot;
    }


    @Override
    public void unlock( Statement statement ) {
        LockManager.INSTANCE.unlock( statement.getTransaction() );
    }


    @Override
    protected void lock( Statement statement ) throws DeadlockException {
        LockManager.INSTANCE.lock( LockMode.EXCLUSIVE, statement.getTransaction() );
    }


    @Override
    public String getQuery( Node parsed, QueryParameters parameters ) {
        return parameters.getQuery();
    }


    @Override
    public AlgDataType getParameterRowType( Node left ) {
        return null;
    }


    @Override
    public List<String> splitStatements( String statements ) {
        List<String> split = new ArrayList<>();
        Stack<Character> brackets = new Stack<>();
        StringBuilder currentStatement = new StringBuilder();
        Character quote = null;

        for ( int i = 0; i < statements.length(); i++ ) {
            char ch = statements.charAt( i );

            if ( quote != null && ch == quote ) {
                if ( i + 1 == statements.length() || statements.charAt( i + 1 ) != quote ) {
                    quote = null;
                } else {
                    currentStatement.append( quote );
                    currentStatement.append( quote );
                    i += 1;
                    continue;
                }
            } else if ( quote == null ) {
                if ( ch == '\'' || ch == '"' ) {
                    quote = ch;
                } else if ( ch == '(' || ch == '[' || ch == '{' ) {
                    brackets.push( ch == '(' ? ')' : ch == '[' ? ']' : '}' );
                } else if ( ch == ')' || ch == ']' || ch == '}' ) {
                    if ( ch != brackets.pop() ) {
                        throw new GenericRuntimeException( "Unbalanced brackets" );
                    }
                } else if ( ch == ';' ) {
                    if ( !brackets.isEmpty() ) {
                        throw new GenericRuntimeException( "Missing " + brackets.pop() );
                    }
                    split.add( currentStatement.toString() );
                    currentStatement = new StringBuilder();
                    continue;
                } else if ( ch == '/' && i + 1 < statements.length() && statements.charAt( i + 1 ) == '/' ) {
                    i += 1;
                    while ( i + 1 < statements.length() && statements.charAt( i + 1 ) != '\n' ) {
                        i++;
                    }
                    // i + 1 < statements.length() means that statements.charAt( i + 1 ) == '\n'
                    if ( i + 1 < statements.length() ) {
                        i++;
                    }
                    // This whitespace prevents constructions like "SEL--\nECT" from resulting in valid SQL
                    ch = ' ';
                } else if ( ch == '/' && i + 1 < statements.length() && statements.charAt( i + 1 ) == '*' ) {
                    i += 2;
                    while ( i + 1 < statements.length() && !(statements.charAt( i ) == '*' && statements.charAt( i + 1 ) == '/') ) {
                        i++;
                    }
                    if ( i + 1 == statements.length() ) {
                        throw new GenericRuntimeException( "Unterminated comment" );
                    }
                    i++;
                    // Same reason as above for cases like "SEL/**/ECT"
                    ch = ' ';
                }
            }
            currentStatement.append( ch );
        }

        if ( quote != null ) {
            throw new GenericRuntimeException( String.format( "Unterminated %s", quote ) );
        }

        if ( !brackets.empty() ) {
            throw new GenericRuntimeException( "Missing " + brackets.pop() );
        }

        if ( !currentStatement.toString().isBlank() ) {
            split.add( currentStatement.toString() );
        }

        return split.stream().map( String::strip ).toList();
    }

}
