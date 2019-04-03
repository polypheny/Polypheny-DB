package ch.unibas.dmi.dbis.polyphenydb.config;

public class ConfigNumber extends Config<Number> {

    private Number value;
    private ConfigValidator validationMethod;

    public ConfigNumber ( String key ) {
        super( key );
        super.setConfigType( "Number" );
    }

    public ConfigNumber ( String key, String description ) {
        super( key, description );
    }

    public Number getValue() {
        return this.value;
    }

    public void setValue( Number v ) {
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

    public ConfigNumber withUi ( int webUiGroup, WebUiFormType type ) {
        super.withUi(webUiGroup, type);
        return this;
    }

    public ConfigNumber withJavaValidation (ConfigValidator c) {
        this.validationMethod = c;
        return this;
    }

    public String toString() {
        return super.toString();
    }

    public interface ConfigValidator {
        boolean validate ( Number a );
    }

}
