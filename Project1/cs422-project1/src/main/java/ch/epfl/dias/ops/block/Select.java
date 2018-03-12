package ch.epfl.dias.ops.block;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.column.DBColumn;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;

public class Select implements BlockOperator {
	private BlockOperator child;
	private BinaryOp op;
	private int fieldNo;
	private int value;

	public Select(BlockOperator child, BinaryOp op, int fieldNo, int value) {
		this.child = child;
		this.op = op;
		this.fieldNo = fieldNo;
		this.value = value;
	}

	@Override
	public DBColumn[] execute() {
		DBColumn[] columns = child.execute();
		Set<Integer> validIndex = new HashSet<>();
		DBColumn filteredColumn = getValidIndices(columns[fieldNo], validIndex);

		DBColumn[] result = new DBColumn[columns.length];
		for (int i=0; i < result.length; i++) {
			if (i != fieldNo)
				result[i] = filterColumn(columns[i], validIndex);
			else
				result[i] = filteredColumn;
		}

		return result;
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
						case EQ:
							isValid = attr[i] == value;
							break;
						case GT:
							isValid = attr[i] > value;
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
