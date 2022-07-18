package org.polypheny.db.languages.polyscript;

public class MqlExpression extends Expression{

    public MqlExpression(String value) {
        super(value);
    }

    @Override
    Expression newInstance(String script) {
        return new MqlExpression(script);
    }
}
