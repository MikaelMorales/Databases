package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.column.DBColumn;

import java.util.ArrayList;
import java.util.List;

public class Select implements VectorOperator {

	private VectorOperator child;
	private BinaryOp op;
	private int fieldNo;
	private int value;
	private DBColumn[] cacheColumns;
	private int vectorSize = -1;

	public Select(VectorOperator child, BinaryOp op, int fieldNo, int value) {
		this.child = child;
		this.op = op;
		this.fieldNo = fieldNo;
		this.value = value;
	}
	
	@Override
	public void open() {
		child.open();
	}

	@Override
	public DBColumn[] next() {
		DBColumn[] columns;
		int currentSize = 0;

		if(cacheColumns != null) {
			columns = cacheColumns.clone();
			cacheColumns = null;
		} else {
			columns = child.next();
		}

		if (columns[0].eof) {
			return columns;
		}

		if (vectorSize == -1)
			vectorSize = columns[0].attributes == null ? 0 : columns[0].attributes.length;

		DBColumn[] result = new DBColumn[columns.length];

		while (!columns[0].eof && vectorSize != currentSize) {
			List<Integer> validIndices = new ArrayList<>();
			Integer[] attr = columns[fieldNo].getAsInteger();

			int index = 0;
			while (index < attr.length && currentSize < vectorSize) {
				if(isValidIndex(attr[index])) {
					validIndices.add(index);
					currentSize++;
				}
				index++;
			}

			for (int i = 0; i < result.length; i++) {
				result[i] = fillColumns(result[i], columns[i], validIndices);
			}

			if (index != attr.length) {
				cacheColumns = new DBColumn[columns.length];
				for (int i = 0; i < result.length; i++) {
					cacheColumns[i] = fillCache(columns[i], index);
				}
			}

			if (vectorSize != currentSize)
				columns = child.next();
		}

		if (columns[0].eof && result[0].attributes.length == 0) {
			for (int i=0; i < columns.length; i++) {
				result[i] = new DBColumn();
			}
		}
		return result;
	}

	@Override
	public void close() {
		child.close();
	}

	private DBColumn fillCache(DBColumn col, Integer start) {
		Object[] cache = new Object[col.attributes.length - start];
		System.arraycopy(col.attributes, start, cache, 0, col.attributes.length - start);
		return new DBColumn(cache, col.type);
	}

	private DBColumn fillColumns(DBColumn res, DBColumn col, List<Integer> validIndices) {
		int size = 0;
		if (res != null && res.attributes != null)
			size = res.attributes.length;
		Object[] result = new Object[size + validIndices.size()];
		if (size != 0) {
			System.arraycopy(res.attributes, 0, result, 0, size);
		}

		Object[] attr = col.attributes;
		for (int i=0; i < validIndices.size(); i++) {
			result[size+i] = attr[validIndices.get(i)];
		}

		return new DBColumn(result, col.type);
	}

	private boolean isValidIndex(Integer i) {
		switch (op) {
			case GE:
				return i >= value;
			case GT:
				return i > value;
			case EQ:
				return i == value;
			case LE:
				return i <= value;
			case LT:
				return i < value;
			case NE:
				return i != value;
			default:
				return false;
		}
	}
}
