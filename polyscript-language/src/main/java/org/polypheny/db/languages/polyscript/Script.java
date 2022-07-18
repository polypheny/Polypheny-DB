package org.polypheny.db.languages.polyscript;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class Script {
    private final List<Expression> expressions;

    public Script(List<Expression> expressions) {
        this.expressions = expressions;
    }

    public Script parameterize(Map<String, Object> arguments) {
        List<Expression> parameterizedExpressions = new ArrayList<>();
        for(Expression expression : expressions) {
            Expression parameterizedExpression = expression.parameterize(arguments);
            parameterizedExpressions.add(parameterizedExpression);
        }
        return new Script(parameterizedExpressions);
    }

    public Stream<Expression> stream() {
        return expressions.stream();
    }

    @Override
    public String toString() {
        return expressions.toString();
    }
}
