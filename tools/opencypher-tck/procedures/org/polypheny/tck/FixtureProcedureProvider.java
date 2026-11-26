package org.polypheny.tck;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.polypheny.db.languages.CypherTableProcedureProvider;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyValue;


/** Filesystem fixture extension, loaded only on the isolated test server's extra classpath. */
public final class FixtureProcedureProvider implements CypherTableProcedureProvider {

    private static final Pattern NAMESPACE = Pattern.compile( "tck_[0-9a-f]{32}" );
    private static final Pattern SIGNATURE = Pattern.compile( "([\\w.]+)\\(([^()]*)\\)\\s*::\\s*\\(([^()]*)\\)" );
    private static final Pattern FIELD = Pattern.compile( "(\\w+)\\s*::\\s*([A-Z]+)(\\?)?" );
    private final ObjectMapper mapper = new ObjectMapper();
    private final String directory = System.getenv( "POLYPHENY_TCK_PROCEDURES_DIR" );


    @Override
    public Optional<Table> find( String graphNamespace, List<String> qualifiedName ) {
        if ( directory == null || !NAMESPACE.matcher( graphNamespace ).matches() ) {
            return Optional.empty();
        }
        Path fixture = Path.of( directory, graphNamespace + ".json" );
        if ( !Files.isRegularFile( fixture, LinkOption.NOFOLLOW_LINKS ) ) {
            return Optional.empty();
        }
        try ( var input = Files.newInputStream( fixture, LinkOption.NOFOLLOW_LINKS ) ) {
            JsonNode procedures = mapper.readTree( input ).path( "procedures" );
            for ( JsonNode entry : procedures ) {
                var signature = SIGNATURE.matcher( entry.path( "signature" ).asText().strip() );
                if ( !signature.matches() ) {
                    throw new IllegalArgumentException( "Invalid TCK procedure signature." );
                }
                if ( signature.group( 1 ).equals( String.join( ".", qualifiedName ) ) ) {
                    return Optional.of( table( entry, fields( signature.group( 2 ) ), fields( signature.group( 3 ) ) ) );
                }
            }
            return Optional.empty();
        } catch ( IOException e ) {
            throw new UncheckedIOException( "Could not read TCK procedure fixture.", e );
        }
    }


    private static List<Field> fields( String text ) {
        if ( text.isBlank() ) {
            return List.of();
        }
        return Arrays.stream( text.split( "," ) ).map( entry -> {
            var field = FIELD.matcher( entry.strip() );
            if ( !field.matches() ) {
                throw new IllegalArgumentException( "Invalid TCK procedure field: " + entry );
            }
            PolyType type = switch ( field.group( 2 ) ) {
                case "INTEGER" -> PolyType.BIGINT;
                case "STRING" -> PolyType.VARCHAR;
                case "FLOAT" -> PolyType.DOUBLE;
                case "NUMBER" -> PolyType.DECIMAL;
                default -> throw new IllegalArgumentException( "Unsupported TCK procedure field type: " + field.group( 2 ) );
            };
            return new Field( field.group( 1 ), type, field.group( 3 ) != null );
        } ).toList();
    }


    private static Table table( JsonNode entry, List<Field> inputs, List<Field> outputs ) {
        List<String> names = Stream.concat( inputs.stream(), outputs.stream() ).map( Field::name ).toList();
        List<String> columns = new ArrayList<>();
        entry.path( "columns" ).forEach( cell -> columns.add( cell.asText() ) );
        if ( !columns.equals( names ) ) {
            throw new IllegalArgumentException( "TCK procedure columns do not match the signature." );
        }
        List<List<PolyValue>> rows = new ArrayList<>();
        for ( JsonNode row : entry.path( "rows" ) ) {
            List<PolyValue> values = new ArrayList<>();
            row.forEach( cell -> values.add( cell.isNull() ? PolyNull.NULL : PolyValue.fromJson( cell.toString() ) ) );
            rows.add( values );
        }
        return new Table( inputs, outputs, rows );
    }

}
