/*
 * Copyright 2019-2022 The Polypheny Project
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

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.languages.polyscript.PolyScriptNode;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.piglet.Ast;
import org.polypheny.db.piglet.parser.ParseException;
import org.polypheny.db.piglet.parser.PigletParser;
import org.polypheny.db.polyscript.parser.PolyScript;
import org.polypheny.db.transaction.*;
import org.polypheny.db.util.DeadlockException;
import org.polypheny.db.util.Pair;

import java.io.StringReader;
import java.util.List;

@Slf4j
public class PolyScriptProcessorImpl extends Processor {
    private String query;

    @Override
    public Node parse(String query) {
        this.query = query;
        final StopWatch stopWatch = new StopWatch();
        if ( log.isDebugEnabled() ) {
            log.debug( "Parsing PolyScript statement ..." );
        }
        stopWatch.start();
        List<String> parsed;
        if ( log.isDebugEnabled() ) {
            log.debug( "POLYSCRIPT: {}", query );
        }

        try {
            parsed = new PolyScript( new StringReader( query ) ).Start();
        } catch ( Exception e ) {
            log.error( "Caught exception", e );
            throw new RuntimeException( e );
        }
        stopWatch.stop();
        if ( log.isTraceEnabled() ) {
            log.trace( "Parsed query: [{}]", parsed );
        }
        if ( log.isDebugEnabled() ) {
            log.debug( "Parsing PolyScript statement ... done. [{}]", stopWatch );
        }
        return new PolyScriptNode("");
    }

    @Override
    public Pair<Node, AlgDataType> validate( Transaction transaction, Node parsed, boolean addDefaultValues ) {
        throw new RuntimeException( "The PolyScript implementation does not support validation." );
    }

    @Override
    public AlgRoot translate(Statement statement, Node query, QueryParameters parameters) {
//        final PigAlgBuilder builder = PigAlgBuilder.create( statement );
//        new Handler( builder ).handle( (Ast.PigNode) query );
//        return AlgRoot.of( builder.build(), Kind.SELECT );
        throw new RuntimeException( "The PolyScript implementation does not support translation." );
    }

    @Override
    public void unlock(Statement statement) {
        LockManager.INSTANCE.unlock( LockManager.GLOBAL_LOCK, (TransactionImpl) statement.getTransaction() );
    }

    @Override
    protected void lock(Statement statement) throws DeadlockException {
        LockManager.INSTANCE.lock( LockManager.GLOBAL_LOCK, (TransactionImpl) statement.getTransaction(), Lock.LockMode.EXCLUSIVE );
    }

    @Override
    public String getQuery(Node parsed, QueryParameters parameters) {
        return query;
    }

    @Override
    public AlgDataType getParameterRowType(Node left) {
        return null;
    }
}
