package ch.unibas.dmi.dbis.polyphenydb.config;

public class ConfigInteger extends Config<Integer> {

    private Integer value;
    private ConfigValidator validationMethod;

    public ConfigInteger( String key ) {
        super( key );
        super.setConfigType( "Integer" );
    }

    public ConfigInteger( String key, String description ) {
        super( key, description );
    }

    public Integer getValue() {
        return this.value;
    }

    public void setValue( Integer v ) {
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

    public ConfigInteger withUi ( int webUiGroup, WebUiFormType type ) {
        super.withUi(webUiGroup, type);
        return this;
    }

    public ConfigInteger withJavaValidation (ConfigValidator c) {
        this.validationMethod = c;
        return this;
    }

    public String toString() {
        return super.toString();
    }

    public interface ConfigValidator {
        boolean validate ( Integer a );
    }

}
