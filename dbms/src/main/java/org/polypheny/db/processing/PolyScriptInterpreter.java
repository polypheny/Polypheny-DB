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

import org.jetbrains.annotations.NotNull;
import org.polypheny.db.PolyResult;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.languages.mql.*;
import org.polypheny.db.languages.polyscript.*;
import org.polypheny.db.polyscript.parser.ParseException;
import org.polypheny.db.polyscript.parser.PolyScript;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringReader;
import java.util.Map;
import java.util.stream.Collectors;

public class PolyScriptInterpreter implements ScriptInterpreter {

    private final SqlProcessorFacade sqlProcessorFacade;

    private final MqlProcessorFacade mqlProcessorFacade;
    private final Transaction transaction;

    private final Logger logger = LoggerFactory.getLogger(PolyScriptInterpreter.class);

    // TODO(nic): Pass MqlProcessorFacade outside. Makes dependency visible, makes it testable (with mock)
    public PolyScriptInterpreter(SqlProcessorFacade sqlFacade, Transaction transaction) {
        this.sqlProcessorFacade = sqlFacade;
        this.transaction = transaction;
        this.mqlProcessorFacade = new MqlProcessorFacade();
    }

    // TODO(nic): Pass arguments along with Script to parser
    @Override
    public PolyResult interprete(String script, Map<String, Object> arguments) {
        Script parsed;
        try {
            parsed = parseScript(script);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        Script parameterizedScript = parsed.parameterize(arguments);
        return runScript(parameterizedScript);
    }

    private PolyResult runScript(Script parsed) {
        PolyResult result = null;
        // TODO: Not pretty (toList call)
        //parsed.stream().map(this::run).findFirst();
        //parsed.stream().map(this::run).reduce((e1, e2) -> e2).get();

        for (Expression expression : parsed.stream().collect(Collectors.toList())) {
            result = run(expression); // return result of last executed query
        }
        return result;
    }

    private void closeTransaction() {
        try {
            transaction.commit();
        } catch (TransactionException e) {
            throw new RuntimeException(e);
        }
    }

    @NotNull
    private Script parseScript(String script) throws ParseException {
        Script parsed;
        if (wrappedWithQuotes(script)) {
            parsed = new PolyScript(new StringReader(removeWrappingQuotes(script))).Start();
        } else {
            parsed = new PolyScript(new StringReader(script)).Start();
        }
        return parsed;
    }

    private boolean wrappedWithQuotes(String script) {
        return firstCharacterIsQuote(script) && lastCharacterIsQuote(script);
    }

    private boolean firstCharacterIsQuote(String script) {
        return script.startsWith("'");
    }

    private boolean lastCharacterIsQuote(String script) {
        return script.lastIndexOf("'") == script.length() - 1;
    }

    private String removeWrappingQuotes(String script) {
        return script.substring(1, script.length() - 1);
    }

    private PolyResult run(Expression expression) {
        if (expression instanceof SqlExpression) {
            return process((SqlExpression) expression);
        } else if (expression instanceof MqlExpression) {
            return process((MqlExpression) expression);
        } else if (expression instanceof CqlExpression) {
            return process((CqlExpression) expression);
        } else {
            throw new UnsupportedOperationException(
                    String.format("The provided language %s isn't supported by the Interpreter",
                            expression.getClass().getName())
            );
        }
    }

    private PolyResult process(SqlExpression line) {

        return sqlProcessorFacade.runSql(line.getValue(), transaction);
    }

    private PolyResult process(MqlExpression line) {
        return mqlProcessorFacade.process(line, transaction);
    }

    private PolyResult process(CqlExpression line) {
        throw new UnsupportedOperationException();
    }


}
