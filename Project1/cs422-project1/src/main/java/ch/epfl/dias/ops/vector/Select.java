package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.column.DBColumn;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;

public class Select implements VectorOperator {

	private VectorOperator child;
	private BinaryOp op;
	private int fieldNo;
	private int value;

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
		DBColumn[] columns = child.next();
		Set<Integer> validIndices = new HashSet<>();
		DBColumn[] result = new DBColumn[columns.length];

		while (!columns[0].eof && validIndices.isEmpty()) {
			validIndices = new HashSet<>();
			DBColumn filteredColumn = getValidIndices(columns[fieldNo], validIndices);
			for (int i = 0; i < result.length; i++) {
				if (i != fieldNo)
					result[i] = filterColumn(columns[i], validIndices);
				else
					result[i] = filteredColumn;
			}
			if (validIndices.isEmpty())
				columns = child.next();
		}

		if (columns[0].eof) {
			for (int i = 0; i < result.length; i++) {
				result[i] = new DBColumn();
			}
		}
		return result;
	}

	@Override
	public void close() {
		child.close();
	}

	private DBColumn filterColumn(DBColumn col, Set<Integer> validIndex) {
		Object[] attr = col.attributes;
		Object[] filteredAttr = IntStream.range(0, attr.length)
				.filter(validIndex::contains)
				.mapToObj(i -> attr[i])
				.toArray();
		return new DBColumn(filteredAttr, col.type);
	}

	private DBColumn getValidIndices(DBColumn col, Set<Integer> validIndex) {
		Integer[] attr = col.getAsInteger();
		Object[] filteredAttr = IntStream.range(0, attr.length)
				.filter(i -> {
					boolean isValid = false;
					switch (op) {
						case GE:
							isValid = attr[i] >= value;
							break;
						case GT:
							isValid = attr[i] > value;
							break;
						case EQ:
							isValid = attr[i] == value;
							break;
						case LE:
							isValid = attr[i] <= value;
							break;
						case LT:
							isValid = attr[i] < value;
							break;
						case NE:
							isValid = attr[i] != value;
							break;
					}
					if (isValid)
						validIndex.add(i);
					return isValid;
				})
				.mapToObj(i -> attr[i])
				.toArray();

		return new DBColumn(filteredAttr, col.type);
	}
}
