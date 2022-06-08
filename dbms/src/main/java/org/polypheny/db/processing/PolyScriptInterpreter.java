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

import org.polypheny.db.PolyResult;
import org.polypheny.db.polyscript.parser.ParseException;
import org.polypheny.db.polyscript.parser.PolyScript;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionManager;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class PolyScriptInterpreter implements ScriptInterpreter {

    private final SqlProcessorFacade sqlProcessorFacade;

    private final Transaction transaction;

    public PolyScriptInterpreter(SqlProcessorFacade sqlFacade, Transaction transaction) {
        this.sqlProcessorFacade = sqlFacade;
        this.transaction = transaction;
    }

    @Override
    public PolyResult interprete(String script) {
        int LANGUAGE_PREFIX = 3;
        int RPAREN_AND_SEMICOLON = 2;
        int LEFT_PAREN = 1;
        List<String> parsed;
        try {
            if(wrappedWithQuotes(script)) {
                parsed = new PolyScript(new StringReader(removeWrappingQuotes(script))).Start();

            } else {
                parsed = new PolyScript(new StringReader(script)).Start();
            }
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        PolyResult result = null;
        for (String line : parsed) {
            //String language = line.substring(0, LANGUAGE_PREFIX - 1);
            String code = line.substring(LANGUAGE_PREFIX + LEFT_PAREN, line.length() - RPAREN_AND_SEMICOLON);
            result = run(line); // return result of last executed query
        }
        return result;
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

    private PolyResult run(String line) {
        return process(line);
    }

    private PolyResult run(String language, String line) {
        switch (language) {
            case "SQL":
                return process(line);
            case "MQL":
                return process(line);
            case "CQL":
                return process(line);
            default:
                throw new UnsupportedOperationException(
                        String.format("The provided language %s isn't supported by the Interpreter", language)
                );
        }
    }

    private PolyResult process(String line) {
        return sqlProcessorFacade.runSql(line, transaction);
    }


}
