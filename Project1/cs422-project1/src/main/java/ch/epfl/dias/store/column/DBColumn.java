package ch.epfl.dias.store.column;

import ch.epfl.dias.store.DataType;

import java.util.ArrayList;
import java.util.List;

public class DBColumn {
	public final List<Object> attributes = new ArrayList<Object>();
	public DataType type;
	public boolean eof;

	public DBColumn(DataType type) {
		this.type = type;
		this.eof = false;
	}

	public DBColumn() {
		this.eof = true;
	}

	public void append(Object o) {
		attributes.add(o);
	}

	public int[] getAsInteger() {
		int[] column = new int[attributes.size()];
		for (int i = 0; i < column.length; i++) {
				column[i] = (Integer) attributes.get(i);
		}
		return column;
	}

	public double[] getAsDouble() {
		double[] column = new double[attributes.size()];
		for (int i = 0; i < column.length; i++) {
			column[i] = (Double) attributes.get(i);
		}
		return column;
	}

	public String[] getAsString() {
		String[] column = new String[attributes.size()];
		for (int i = 0; i < column.length; i++) {
			column[i] = (String) attributes.get(i);
		}
		return column;
	}

	public Boolean[] getAsBoolean() {
		Boolean[] column = new Boolean[attributes.size()];
		for (int i = 0; i < column.length; i++) {
			column[i] = (Boolean) attributes.get(i);
		}
		return column;
	}

	@Override
	public String toString() {
		if (this.eof)
			return "EOF";

		StringBuilder sb = new StringBuilder();
		for (Object field : attributes) {
			sb.append(field.toString());
			sb.append("\n");
		}
		return sb.toString();
	}
}
