package ch.unibas.dmi.dbis.polyphenydb.config;

public class ConfigString extends ConfigScalar {

    private String value;
    private ConfigValidator validationMethod;

    public ConfigString ( String key ) {
        super( key );
        super.setConfigType( "String" );
    }

    public ConfigString ( String key, String description ) {
        super( key, description );
        super.setConfigType( "String" );
    }

    @Override
    public Object getObject() {
        return this.value;
    }

    @Override
    public void setObject( Object o ) {
        if(o == null){
            this.value = null;
            return;
        }
        String s = o.toString();
        if( validate( s ) ){
            this.value = s;
        }
    }

    @Override
    public String getString() {
        return this.value;
    }

    @Override
    public void setString( String s ) {
        if ( validate( s ) ){
            this.value = s;
        }
    }

    private boolean validate ( String s ) {
        if ( this.validationMethod != null ) {
            if( this.validationMethod.validate( s ) ) {
                return true;
            } else {
                System.out.println( "Java validation: false." );
                return false;
            }
        } //else if (this.validationMethod == null ) {
        else{
            return true;
        }
    }

    public ConfigString withJavaValidation (ConfigValidator c) {
        this.validationMethod = c;
        return this;
    }

    public interface ConfigValidator {
        boolean validate ( String a );
    }

}
