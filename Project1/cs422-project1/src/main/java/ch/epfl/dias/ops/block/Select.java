package ch.epfl.dias.ops.block;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;

import java.util.Arrays;

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
		DBColumn col = child.execute()[fieldNo];
		if (col.eof)
			return new DBColumn[]{new DBColumn()};

		Integer[] attribute = col.getAsInteger();
		Object[] res = Arrays.stream(attribute).filter(attr -> {
            switch (op) {
                case GE:
                    return attr >= value;
                case EQ:
                    return attr == value;
                case GT:
                    return attr > value;
                case LE:
                    return attr <= value;
                case LT:
                    return attr < value;
                case NE:
                    return attr != value;
                default: return false;
            }
        }).toArray();

		return new DBColumn[]{new DBColumn(res, DataType.INT)};
	}
}
