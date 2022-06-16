package org.polypheny.db.languages.polyscript;

import java.util.List;
import java.util.stream.Stream;

public class Script {
    private final List<Expression> expressions;

    public Script(List<Expression> expressions) {
        this.expressions = expressions;
    }

    public Stream<Expression> stream() {
        return expressions.stream();
    }

    @Override
    public String toString() {
        return expressions.toString();
    }
}
