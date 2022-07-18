package org.polypheny.db.languages.polyscript;

public class CqlExpression extends Expression{

    public CqlExpression(String value) {
        super(value);
    }

    @Override
    Expression newInstance(String script) {
        return new CqlExpression(script);
    }
}
