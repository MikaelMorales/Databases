package ch.epfl.dias.store;

import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import java.io.IOException;

/**
 * This class abstracts the functionality for 
 * Storage models (NSM, DSM and PAX)
 *
 */
public abstract class Store {

	/**
	 * Load the data into the data structures of the store
	 */
	public abstract void load();

	/**
	 * Method to access rows available only for row store and PAX
	 * 
	 * @param rownumber
	 * @return
	 */
	public DBTuple getRow(int rownumber) {
		return null;
	};

	/**
	 * Method to access columns available only for column store
	 * 
	 * @param columnsToGet
	 *            (the set of columns to get)
	 * @return
	 */
	public DBColumn[] getColumns(int[] columnsToGet) {
		return null;
	};


	/**
	 * Method that check if a tuple contains the correct amount of
	 * expected attributed
	 * @param tuple The tuple to check
	 * @param schema The schema of the table
	 *
	 * @throws IOException if a parsed tuple doesn't have the correct amount of attributes
	 */
	protected void checkNumberOfAttributes(String[] tuple, DataType[] schema) throws IOException {
		if (tuple.length != schema.length) {
			throw new IOException("The row doesn't contain all the attributes");
		}
	}

	/**
	 * Parse the given tuple, respecting the given schema.
	 *
	 * @param tuple Tuple to parse
	 * @param schema The schema of the table
	 * @return tuple correctly parsed, wrapped up in an Object array.
	 */
	protected Object[] parseDataWithType(String[] tuple, DataType[] schema) {
		Object[] output = new Object[tuple.length];
		for (int i=0; i < tuple.length; i++) {
			switch(schema[i]) {
				case INT: {
					output[i] = Integer.parseInt(tuple[i]);
					break;
				}
				case STRING: {
					output[i] = tuple[i];
					break;
				}
				case DOUBLE: {
					output[i] = Double.parseDouble(tuple[i]);
					break;
				}
				case BOOLEAN: {
					output[i] = Boolean.parseBoolean(tuple[i]);
					break;
				}
				default: throw new IllegalArgumentException("Invalid type");
			}
		}
		return output;
	}
}
