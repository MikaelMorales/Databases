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
		double value = 0.0;
		switch (agg) {
			case COUNT:
				value = col.attributes.length;
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

	private double getAVG(DBColumn col) {
		DataType type = col.type;
		OptionalDouble value;
		if (type == DataType.INT) {
			value = Arrays.stream(col.getAsInteger()).mapToDouble(a -> a).average();
		} else if (type == DataType.DOUBLE) {
			value = Arrays.stream(col.getAsDouble()).mapToDouble(a -> a).average();
		} else {
			throw new IllegalArgumentException("Invalid type to compute sum");
		}
		return value.orElse(0.0);
	}

	private double getMAX(DBColumn col) {
		DataType type = col.type;
		if (type == DataType.INT) {
			Optional<Integer> value = Arrays.stream(col.getAsInteger()).max(Integer::compareTo);
			return value.orElse(0).doubleValue();
		} else if (type == DataType.DOUBLE) {
			Optional<Double> value = Arrays.stream(col.getAsDouble()).max(Double::compareTo);
			return value.orElse(0.0);
		} else {
			throw new IllegalArgumentException("Invalid type to compute sum");
		}
	}

	private double getMIN(DBColumn col) {
		DataType type = col.type;
		if (type == DataType.INT) {
			Optional<Integer> value = Arrays.stream(col.getAsInteger()).min(Integer::compareTo);
			return value.orElse(0).doubleValue();
		} else if (type == DataType.DOUBLE) {
			Optional<Double> value = Arrays.stream(col.getAsDouble()).min(Double::compareTo);
			return value.orElse(0.0);
		} else {
			throw new IllegalArgumentException("Invalid type to compute sum");
		}
	}

	private double getSUM(DBColumn col) {
		DataType type = col.type;
		if (type == DataType.INT) {
			return Arrays.stream(col.getAsInteger()).mapToInt(i -> i).sum();
		} else if (type == DataType.DOUBLE) {
			return Arrays.stream(col.getAsDouble()).mapToDouble(i -> i).sum();
		} else {
			throw new IllegalArgumentException("Invalid type to compute sum");
		}
	}

	private DBColumn createDBColumn(double value) {
		switch (agg) {
			case COUNT:
				return new DBColumn(new Object[]{(int) value}, DataType.INT);
			case AVG:
				return new DBColumn(new Object[]{value}, DataType.DOUBLE);
			case MAX:
			case MIN:
			case SUM:
				if(dt == DataType.INT) {
					return new DBColumn(new Object[]{(int)value}, dt);
				} else if (dt == DataType.DOUBLE){
					return new DBColumn(new Object[]{value}, dt);
				}
			default: throw new IllegalArgumentException("Can't compute MAX, MIN or SUM of type " + dt.name());
		}
	}
}
