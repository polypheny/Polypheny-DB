package ch.unibas.dmi.dbis.polyphenydb.config;

public class ConfigNumber extends Config {

    private Number value;
    private ConfigValidator validationMethod;

    public ConfigNumber ( String key ) {
        super( key );
        super.setConfigType( "Number" );
    }

    public ConfigNumber ( String key, String description ) {
        super( key, description );
        super.setConfigType( "Number" );
    }

    @Override
    public Object getObject() {
        return this.value;
    }

    @Override
    public void setObject( Object o ) {
        //todo or parseInt
        if(o == null){
            this.value = null;
            return;
        }
        Number i = (Number) o;
        if( validate( i ) ){
            this.value = i;
        }
    }

    @Override
    public String getString() {
        return this.value.toString();
    }

    @Override
    public void setString( String s ) {
        //todo parse double or int or throw error?
        int i = Integer.parseInt( s );
        if ( validate( i ) ){
            this.value = i;
        }
    }

    @Override
    public int getInt() {
        return this.value.intValue();
    }

    @Override
    public void setInt( int i ) {
        if ( validate( i ) ) {
            this.value = i;
        }
    }

    private boolean validate ( Number i ) {
        if ( this.validationMethod != null ) {
            if( this.validationMethod.validate( i ) ) {
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

    public ConfigNumber withJavaValidation (ConfigValidator c) {
        this.validationMethod = c;
        return this;
    }

    public interface ConfigValidator {
        boolean validate ( Number a );
    }

}
