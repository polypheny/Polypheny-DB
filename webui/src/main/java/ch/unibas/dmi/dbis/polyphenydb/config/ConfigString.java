package ch.unibas.dmi.dbis.polyphenydb.config;

public class ConfigString extends Config<String> {

    private String value;
    private ConfigValidator validationMethod;

    public ConfigString ( String key ) {
        super( key );
        super.setConfigType( "String" );
    }

    public ConfigString ( String key, String description ) {
        super( key, description );
    }

    public String getValue() {
        return this.value;
    }

    public void setValue( String v ) {
        if ( this.validationMethod != null ) {
            if( this.validationMethod.validate( v ) ) {
                this.value = v;
            } else {
                System.out.println( "Java validation: false." );
            }
        } //else if (this.validationMethod == null ) {
        else{
            this.value = v;
        }
    }

    public ConfigString withUi ( int webUiGroup, WebUiFormType type ) {
        super.withUi(webUiGroup, type);
        return this;
    }

    public ConfigString withJavaValidation (ConfigValidator c) {
        this.validationMethod = c;
        return this;
    }

    public String toString() {
        return super.toString();
    }

    public interface ConfigValidator {
        boolean validate ( String a );
    }

}
