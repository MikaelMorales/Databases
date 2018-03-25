package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.row.DBTuple;

public class Select implements VolcanoOperator {

	private VolcanoOperator child;
	private BinaryOp op;
	private int fieldNo;
	private int value;

	public Select(VolcanoOperator child, BinaryOp op, int fieldNo, int value) {
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
	public DBTuple next() {
		DBTuple row = child.next();
		while(!row.eof && !isValid(row.getFieldAsInt(fieldNo))) {
			row = child.next();
		}

		if (row.eof)
			return new DBTuple();
		else
			return row;
	}

	@Override
	public void close() {
		child.close();
	}

	private boolean isValid(Integer attribute) {
		boolean isValid = false;
		switch (op) {
			case EQ:
				isValid = attribute == value;
				break;
			case GE:
				isValid = attribute >= value;
				break;
			case GT:
				isValid = attribute > value;
				break;
			case LE:
				isValid = attribute <= value;
				break;
			case LT:
				isValid = attribute < value;
				break;
			case NE:
				isValid = attribute != value;
				break;
		}

		return isValid;
	}
}
