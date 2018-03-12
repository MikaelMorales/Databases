package ch.epfl.dias.store.column;

import ch.epfl.dias.store.DataType;

public class DBColumn {
	public Object[] attributes;
	public DataType type;

	public DBColumn(Object[] attributes, DataType type) {
		this.attributes = attributes.clone();
		this.type = type;
	}

	public Integer[] getAsInteger() {
		Integer[] column = new Integer[attributes.length];
		for (int i = 0; i < column.length; i++) {
			column[i] = (Integer) attributes[i];
		}
		return column;
	}

	public Double[] getAsDouble() {
		Double[] column = new Double[attributes.length];
		for (int i = 0; i < column.length; i++) {
			column[i] = (Double) attributes[i];
		}
		return column;
	}

	public String[] getAsString() {
		String[] column = new String[attributes.length];
		for (int i = 0; i < column.length; i++) {
			column[i] = (String) attributes[i];
		}
		return column;
	}

	public Boolean[] getAsBoolean() {
		Boolean[] column = new Boolean[attributes.length];
		for (int i = 0; i < column.length; i++) {
			column[i] = (Boolean) attributes[i];
		}
		return column;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (Object field : attributes) {
			sb.append(field.toString());
			sb.append("\n");
		}
		return sb.toString();
	}
}
