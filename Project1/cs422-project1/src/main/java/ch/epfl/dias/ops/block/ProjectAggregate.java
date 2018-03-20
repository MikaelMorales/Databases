package ch.epfl.dias.ops.block;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;

import java.util.Arrays;
import java.util.Optional;
import java.util.OptionalDouble;

public class ProjectAggregate implements BlockOperator {

	private BlockOperator child;
	private Aggregate agg;
	private DataType dt;
	private int fieldNo;
	
	public ProjectAggregate(BlockOperator child, Aggregate agg, DataType dt, int fieldNo) {
		this.child = child;
		this.agg = agg;
		this.dt = dt;
		this.fieldNo = fieldNo;
	}

	@Override
	public DBColumn[] execute() {
		DBColumn col = child.execute()[fieldNo];
		checkValidOperation(col);
		Object value = setupInitialValue(col);
		switch (agg) {
			case COUNT:
				value = (double) col.attributes.length;
				break;
			case AVG:
				value = getAVG(col);
				break;
			case MAX:
				value = getMAX(col);
				break;
			case MIN:
				value = getMIN(col);
				break;
			case SUM:
				value = getSUM(col);
				break;
		}
		return new DBColumn[]{createDBColumn(value)};
	}

	private Object getAVG(DBColumn col) {
		DataType type = col.type;
		OptionalDouble value;
		if (type == DataType.INT) {
			value = Arrays.stream(col.getAsInteger()).mapToDouble(a -> a).average();
		} else if (type == DataType.DOUBLE) {
			value = Arrays.stream(col.getAsDouble()).mapToDouble(a -> a).average();
		} else {
			throw new IllegalArgumentException("Invalid type to compute average");
		}
		return value.orElse(0.0);
	}

	private Object getMAX(DBColumn col) {
		DataType type = col.type;
		if (type == DataType.INT) {
			Optional<Integer> value = Arrays.stream(col.getAsInteger()).max(Integer::compareTo);
			return value.orElse(0).doubleValue();
		} else if (type == DataType.DOUBLE) {
			Optional<Double> value = Arrays.stream(col.getAsDouble()).max(Double::compareTo);
			return value.orElse(0.0);
		} else if (type == DataType.STRING) {
			Optional<String> value = Arrays.stream(col.getAsString()).max(String::compareTo);
			return value.orElse(null);
		} else {
			throw new IllegalArgumentException("Invalid type to compute max");
		}
	}

	private Object getMIN(DBColumn col) {
		DataType type = col.type;
		if (type == DataType.INT) {
			Optional<Integer> value = Arrays.stream(col.getAsInteger()).min(Integer::compareTo);
			return value.orElse(0).doubleValue();
		} else if (type == DataType.DOUBLE) {
			Optional<Double> value = Arrays.stream(col.getAsDouble()).min(Double::compareTo);
			return value.orElse(0.0);
		} else if (type == DataType.STRING) {
			Optional<String> value = Arrays.stream(col.getAsString()).min(String::compareTo);
			return value.orElse(null);
		} else {
			throw new IllegalArgumentException("Invalid type to compute min");
		}
	}

	private Object getSUM(DBColumn col) {
		DataType type = col.type;
		if (type == DataType.INT) {
			return (double) Arrays.stream(col.getAsInteger()).mapToInt(i -> i).sum();
		} else if (type == DataType.DOUBLE) {
			return Arrays.stream(col.getAsDouble()).mapToDouble(i -> i).sum();
		} else {
			throw new IllegalArgumentException("Invalid type to compute sum");
		}
	}

	private DBColumn createDBColumn(Object value) {
		if(dt == DataType.INT) {
			return new DBColumn(new Object[]{((Double)value).intValue()}, dt);
		} else if (dt == DataType.DOUBLE) {
			return new DBColumn(new Object[]{(Double) value}, dt);
		} else if (dt == DataType.STRING) {
			return new DBColumn(new Object[]{(String) value}, dt);
		} else {
			throw new IllegalArgumentException("Aggregation with invalid return data type " + dt.name());
		}
	}

	private Object setupInitialValue(DBColumn col) {
		switch (agg) {
			case AVG:
				return 0.0;
			case SUM:
				return 0.0;
			case COUNT:
				return 0.0;
			case MAX:
				if (col.type == DataType.DOUBLE || col.type == DataType.INT)
					return Double.MIN_VALUE;
				else
					return null;
			case MIN:
				if (col.type == DataType.DOUBLE || col.type == DataType.INT)
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
