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
		double value = setupInitialValue();
		double numberOfRows = 0.0;
		while (!col.eof) {
			switch (agg) {
				case AVG:
					value += getSUM(col);
					break;
				case COUNT:
					value += col.attributes.length;
					break;
				case MAX:
					value = getMAX(col, value);
					break;
				case MIN:
					value = getMIN(col, value);
					break;
				case SUM:
					value += getSUM(col);
					break;
			}
			numberOfRows += col.attributes.length;
			col = child.next()[fieldNo];
		}
		return new DBColumn[]{createDBColumn(value, numberOfRows)};
	}

	@Override
	public void close() {
		child.close();
	}


	private double getMAX(DBColumn col, double value) {
		DataType type = col.type;
		if (type == DataType.INT) {
			Optional<Integer> newvalue = Arrays.stream(col.getAsInteger()).max(Integer::compareTo);
			double max = newvalue.orElse(0).doubleValue();
			return value > max ? value : max;
		} else if (type == DataType.DOUBLE) {
			Optional<Double> newvalue = Arrays.stream(col.getAsDouble()).max(Double::compareTo);
			double max = newvalue.orElse(0.0);
			return value > max ? value : max;
		} else {
			throw new IllegalArgumentException("Invalid type with max aggregation");
		}
	}

	private double getMIN(DBColumn col, double value) {
		DataType type = col.type;
		if (type == DataType.INT) {
			Optional<Integer> newvalue = Arrays.stream(col.getAsInteger()).min(Integer::compareTo);
			double max = newvalue.orElse(0).doubleValue();
			return value < max ? value : max;
		} else if (type == DataType.DOUBLE) {
			Optional<Double> newvalue = Arrays.stream(col.getAsDouble()).min(Double::compareTo);
			double max = newvalue.orElse(0.0);
			return value < max ? value : max;
		} else {
			throw new IllegalArgumentException("Invalid type with min aggregation");
		}
	}

	private double getSUM(DBColumn col) {
		DataType type = col.type;
		if (type == DataType.INT) {
			return Arrays.stream(col.getAsInteger()).mapToInt(i -> i).sum();
		} else if (type == DataType.DOUBLE) {
			return Arrays.stream(col.getAsDouble()).mapToDouble(i -> i).sum();
		} else {
			throw new IllegalArgumentException("Invalid type with sum aggregation");
		}
	}

	private DBColumn createDBColumn(double value, double numberOfRows) {
		if (agg == Aggregate.AVG) {
				value = numberOfRows == 0 ? 0 : value/numberOfRows;
		}

		if(dt == DataType.INT) {
			return new DBColumn(new Object[]{(int)value}, dt);
		} else if (dt == DataType.DOUBLE){
			return new DBColumn(new Object[]{value}, dt);
		} else {
			throw new IllegalArgumentException("Invalid return data type in aggregation: " + dt.name());
		}
	}

	private double setupInitialValue() {
		switch (agg) {
			case AVG:
				return 0.0;
			case COUNT:
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
}
