package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;

import java.util.Arrays;
import java.util.Optional;

public class ProjectAggregate implements VectorOperator {

	private VectorOperator child;
	private Aggregate agg;
	private DataType dt;
	private int fieldNo;
	private double numberOfRows = 0.0;
	private Object value;

	public ProjectAggregate(VectorOperator child, Aggregate agg, DataType dt, int fieldNo) {
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
	public DBColumn[] next() {
		DBColumn col = child.next()[fieldNo];
		checkValidOperation(col);
		value = setupInitialValue(col);
		compute(col);

		while(!col.eof) {
			col = child.next()[fieldNo];
			compute(col);
		}

		return new DBColumn[]{createDBColumn(value, numberOfRows)};
	}

	@Override
	public void close() {
		child.close();
	}

	private void compute(DBColumn col) {
		switch (agg) {
			case AVG:
				value = getSUM(col, value);
				break;
			case COUNT:
				value = (double) col.attributes.length + (Double) value;
				break;
			case MAX:
				value = getMAX(col, value);
				break;
			case MIN:
				value = getMIN(col, value);
				break;
			case SUM:
				value = getSUM(col, value);
				break;
		}
		numberOfRows += col.attributes.length;
	}

	private Object getMAX(DBColumn col, Object value) {
		DataType type = col.type;
		if (type == DataType.INT) {
			Optional<Integer> newvalue = Arrays.stream(col.getAsInteger()).max(Integer::compareTo);
			double max = newvalue.orElse(0).doubleValue();
			return (Double) value > max ? value : max;
		} else if (type == DataType.DOUBLE) {
			Optional<Double> newvalue = Arrays.stream(col.getAsDouble()).max(Double::compareTo);
			double max = newvalue.orElse(0.0);
			return (Double) value > max ? value : max;
		} else if (type == DataType.STRING) {
			Optional<String> newValue = Arrays.stream(col.getAsString()).max(String::compareTo);
			String max = newValue.orElse(null);
			if (max == null) {
				return value;
			} else {
				if (value == null)
					return max;
				else
					return ((String) value).compareTo(max) > 0 ? value : max;
			}
		} else {
			throw new IllegalArgumentException("Invalid type with max aggregation");
		}
	}

	private Object getMIN(DBColumn col, Object value) {
		DataType type = col.type;
		if (type == DataType.INT) {
			Optional<Integer> newvalue = Arrays.stream(col.getAsInteger()).min(Integer::compareTo);
			double min = newvalue.orElse(0).doubleValue();
			return (Double) value < min ? value : min;
		} else if (type == DataType.DOUBLE) {
			Optional<Double> newvalue = Arrays.stream(col.getAsDouble()).min(Double::compareTo);
			double min = newvalue.orElse(0.0);
			return (Double) value < min ? value : min;
		} else if (type == DataType.STRING) {
			Optional<String> newValue = Arrays.stream(col.getAsString()).min(String::compareTo);
			String min = newValue.orElse(null);
			if (min == null) {
				return value;
			} else {
				if (value == null)
					return min;
				else
					return ((String) value).compareTo(min) < 0 ? value : min;
			}
		} else {
			throw new IllegalArgumentException("Invalid type with min aggregation");
		}
	}

	private Object getSUM(DBColumn col, Object value) {
		DataType type = col.type;
		if (type == DataType.INT) {
			int sum = Arrays.stream(col.getAsInteger()).mapToInt(i -> i).sum();
			return sum + (Double) value;
		} else if (type == DataType.DOUBLE) {
			double sum = Arrays.stream(col.getAsDouble()).mapToDouble(i -> i).sum();
			return sum + (Double) value;
		} else {
			throw new IllegalArgumentException("Invalid type with sum aggregation");
		}
	}

	private DBColumn createDBColumn(Object value, double numberOfRows) {
		if (agg == Aggregate.AVG) {
				value = numberOfRows == 0 ? 0 : (Double) value/numberOfRows;
		}

		if(dt == DataType.INT) {
			return new DBColumn(new Object[]{((Double) value).intValue()}, dt);
		} else if (dt == DataType.DOUBLE) {
			return new DBColumn(new Object[]{(Double) value}, dt);
		} else if (dt == DataType.STRING) {
			return new DBColumn(new Object[]{(String)value}, dt);
		} else {
			throw new IllegalArgumentException("Invalid return data type in aggregation: " + dt.name());
		}
	}

	private Object setupInitialValue(DBColumn col) {
		switch (agg) {
			case AVG:
				return 0.0;
			case COUNT:
				return 0.0;
			case SUM:
				return 0.0;
			case MAX:
				if (col.type != null && (col.type == DataType.DOUBLE || col.type == DataType.INT))
					return Double.MIN_VALUE;
				else
					return null;
			case MIN:
				if (col.type != null && (col.type == DataType.DOUBLE || col.type == DataType.INT))
					return Double.MAX_VALUE;
				else
					return null;
			default: return 0.0;
		}
	}

	private void checkValidOperation(DBColumn col) {
		if ((agg == Aggregate.AVG || agg == Aggregate.SUM) && col.type == DataType.STRING) {
			throw new IllegalArgumentException("Invalid Aggregation on data type string");
		}
	}
}
