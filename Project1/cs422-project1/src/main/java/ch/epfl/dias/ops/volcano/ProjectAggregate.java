package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;

public class ProjectAggregate implements VolcanoOperator {

	private VolcanoOperator child;
	private Aggregate agg;
	private DataType dt;
	private int fieldNo;

	public ProjectAggregate(VolcanoOperator child, Aggregate agg, DataType dt, int fieldNo) {
		this.child = child;
		this.agg = agg;
		this.dt = dt;
		this.fieldNo = fieldNo;
	}

	@Override
	public void open() {
		child.open();
	}

	@Override
	public DBTuple next() {
		DBTuple row = child.next();
		checkValidOperation(row);
		Object value = setupInitialValue(row);
		int numberOfRows = 0;
		while (!row.eof) {
			switch (agg) {
				case COUNT:
					value = (Double) value + 1.0;
					break;
				case AVG:
					value = (Double) value + (Double) getFieldValue(row);
					break;
				case MAX: {
					Object newValue = getFieldValue(row);
					if (value == null) {
						value = newValue;
					} else {
						if (row.types[fieldNo] == DataType.STRING) {
							value = ((String) value).compareTo((String) newValue) > 0 ? value : newValue;
						} else {
							value = (Double) value > (Double) newValue ? value : newValue;
						}
					}
					break;
				}
				case MIN: {
					Object newValue = getFieldValue(row);
					if (value == null) {
						value = newValue;
					} else {
						if (row.types[fieldNo] == DataType.STRING) {
							value = ((String) value).compareTo((String) newValue) < 0 ? value : newValue;
						} else {
							value = (Double) value < (Double) newValue ? value : newValue;
						}
					}
					break;
				}
				case SUM:
					value = (Double) value + (Double) getFieldValue(row);
					break;
			}

			numberOfRows++;
			row = child.next();
		}
		return createDBTuple(value, numberOfRows);
	}

	@Override
	public void close() {
		child.close();
	}

	private Object setupInitialValue(DBTuple row) {
		switch (agg) {
			case COUNT:
				return 0.0;
			case AVG:
				return 0.0;
			case MAX:
				if (row.types != null && (row.types[fieldNo] == DataType.DOUBLE || row.types[fieldNo] == DataType.INT))
					return Double.MIN_VALUE;
				else
					return null;
			case MIN:
				if (row.types != null && (row.types[fieldNo] == DataType.DOUBLE || row.types[fieldNo] == DataType.INT))
					return Double.MAX_VALUE;
				else
					return null;
			case SUM:
				return 0.0;
			default: return 0.0;
		}
	}

	private Object getFieldValue(DBTuple row) {
		if (row.types[fieldNo] == DataType.INT) {
			return (double) row.getFieldAsInt(fieldNo);
		} else if (row.types[fieldNo] == DataType.DOUBLE) {
			return row.getFieldAsDouble(fieldNo);
		} else if (row.types[fieldNo] == DataType.STRING) {
			return row.getFieldAsString(fieldNo);
		} else {
			throw new IllegalArgumentException("Invalid data type");
		}
	}

	private DBTuple createDBTuple(Object value, int numberOfRows) {
		if (agg == Aggregate.AVG)
			value = numberOfRows == 0 ? 0.0 : (Double) value/numberOfRows;

		if(dt == DataType.INT) {
			return new DBTuple(new Object[]{((Double)value).intValue()}, new DataType[]{dt});
		} else if (dt == DataType.DOUBLE){
			return new DBTuple(new Object[]{value}, new DataType[]{dt});
		} else if (dt == DataType.STRING){
			return new DBTuple(new Object[]{(String) value}, new DataType[]{dt});
		} else {
			throw new IllegalArgumentException("Aggregation with invalid return data type " + dt.name());
		}
	}

	private void checkValidOperation(DBTuple row) {
		if ((agg == Aggregate.AVG || agg == Aggregate.SUM) && row.types[fieldNo] == DataType.STRING) {
			throw new IllegalArgumentException("Invalid Aggregation on data type string");
		}
	}
}
