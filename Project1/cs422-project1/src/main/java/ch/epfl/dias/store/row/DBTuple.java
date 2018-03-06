package ch.epfl.dias.store.row;

import ch.epfl.dias.store.DataType;

public class DBTuple {
	public Object[] fields;
	public DataType[] types;
	public boolean eof;

	public DBTuple(Object[] fields, DataType[] types) {
		this.fields = fields.clone();
		this.types = types;
		this.eof = false;
	}

	public DBTuple() {
		this.eof = true;
	}

	/**
	 * XXX Assuming that the caller has ALREADY checked the datatype, and has
	 * made the right call
	 *
	 * @param fieldNo
	 *            (starting from 0)
	 * @return cast of field
	 */
	public Integer getFieldAsInt(int fieldNo) {
		return (Integer) fields[fieldNo];
	}

	public Double getFieldAsDouble(int fieldNo) {
		return (Double) fields[fieldNo];
	}

	public Boolean getFieldAsBoolean(int fieldNo) {
		return (Boolean) fields[fieldNo];
	}

	public String getFieldAsString(int fieldNo) {
		return (String) fields[fieldNo];
	}

	@Override
	public String toString() {
		if (this.eof)
			return "EOF";

		StringBuilder sb = new StringBuilder();
		for (Object field : fields) {
			sb.append(field.toString());
			sb.append(", ");
		}
		return sb.toString();
	}
}
