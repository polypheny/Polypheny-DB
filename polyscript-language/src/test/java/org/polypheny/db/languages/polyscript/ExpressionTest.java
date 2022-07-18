package org.polypheny.db.languages.polyscript;

import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ExpressionTest {

    @Test
    public void testExtractNamedArguments() {
        String query = "sql(insert into students VALUES(:id, :name);";

        Expression sut = new SqlExpression(query);

        List<String> namedArguments = sut.getNamedArguments();
        assertEquals(namedArguments.size(), 2);
        assertEquals(namedArguments.get(0), "id");
        assertEquals(namedArguments.get(1), "name");
    }
    @Test
    public void testParameterizeNamedArguments() {
        String query = "sql(insert into students VALUES(:id, :name);";
        Map<String, Object> arguments = Map.of("id", 1, "name", "James");
        Expression sut = new SqlExpression(query);

        Expression parameterizedExpression = sut.parameterize(arguments);
        String parameterizedScript = parameterizedExpression.getValue();

        assertEquals(parameterizedScript, "sql(insert into students VALUES(1, 'James');");
    }
}
