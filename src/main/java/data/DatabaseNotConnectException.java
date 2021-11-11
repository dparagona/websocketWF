package data;
/**
 * Notifies an error  message if driver is not instantiated yet.
 * @author Giovanni Codianni
 * @author Carmine Colarusso
 * @author Chiara Verdone
 *	
 */
public class DatabaseNotConnectException extends Exception {

	/**
	* 
	*/
	private static final long serialVersionUID = 1L;

	public DatabaseNotConnectException(String msg) {
		super(msg);
	}
}
