package ch.unibas.dmi.dbis.polyphenydb.config;

public class ConfigInteger extends Config {

    private Integer value;
    private ConfigValidator validationMethod;

    public ConfigInteger( String key ) {
        super( key );
        super.setConfigType( "Integer" );
    }

    public ConfigInteger( String key, String description ) {
        super( key, description );
        super.setConfigType( "Integer" );
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
        Integer i = (Integer) o;
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
        //todo or throw error
        int i = Integer.parseInt( s );
        if ( validate( i ) ){
            this.value = i;
        }
    }

    @Override
    public int getInt() {
        return this.value;
    }

    @Override
    public void setInt( int i ) {
        if ( validate( i ) ) {
            this.value = i;
        }
    }

    private boolean validate ( int i ) {
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

    public ConfigInteger withJavaValidation (ConfigValidator c) {
        this.validationMethod = c;
        return this;
    }

    public interface ConfigValidator {
        boolean validate ( Integer a );
    }

}
