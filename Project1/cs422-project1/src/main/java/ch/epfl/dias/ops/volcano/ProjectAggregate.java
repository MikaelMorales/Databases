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
		double value = setupInitialValue();
		int numberOfRows = 0;
		while (!row.eof) {
			switch (agg) {
				case COUNT:
					value += 1.0;
					break;
				case AVG:
					value += getFieldValue(row);
					break;
				case MAX: {
					double newValue = getFieldValue(row);
					value = value > newValue ? value : newValue;
					break;
				}
				case MIN: {
					double newValue = getFieldValue(row);
					value = value < newValue ? value : newValue;
					break;
				}
				case SUM:
					value += getFieldValue(row);
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

	private double setupInitialValue() {
		switch (agg) {
			case COUNT:
				return 0.0;
			case AVG:
				return 0.0;
			case MAX:
				return Double.MIN_VALUE;
			case MIN:
				return Double.MAX_VALUE;
			case SUM:
				return 0.0;
			default: return 0.0;
		}
	}
	private double getFieldValue(DBTuple row) {
		if (row.types[fieldNo] == DataType.INT) {
			return row.getFieldAsInt(fieldNo);
		} else if (row.types[fieldNo] == DataType.DOUBLE) {
			return row.getFieldAsDouble(fieldNo);
		} else {
			throw new IllegalArgumentException("Invalid data type");
		}
	}

	private DBTuple createDBTuple(double value, int numberOfRows) {
		switch (agg) {
			case COUNT:
				return new DBTuple(new Object[]{(int) value}, new DataType[]{DataType.INT});
			case AVG:
				return new DBTuple(new Object[]{numberOfRows == 0 ? 0.0 : value/numberOfRows}, new DataType[]{DataType.DOUBLE});
			case MAX:
			case MIN:
			case SUM:
				if(dt == DataType.INT) {
					return new DBTuple(new Object[]{(int)value}, new DataType[]{dt});
				} else if (dt == DataType.DOUBLE){
					return new DBTuple(new Object[]{value}, new DataType[]{dt});
				}
			default: throw new IllegalArgumentException("Can't compute MAX, MIN or SUM of type " + dt.name());
		}
	}
}
