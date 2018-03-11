package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;

public class Project implements VolcanoOperator {

	private VolcanoOperator child;
	private int[] fieldNo;

	public Project(VolcanoOperator child, int[] fieldNo) {
		this.child = child;
		this.fieldNo = fieldNo;
	}

	@Override
	public void open() {
		child.open();
	}

	@Override
	public DBTuple next() {
		DBTuple row = child.next();
		if (row.eof)
			return new DBTuple();

		Object[] filteredTuple = new Object[fieldNo.length];
		DataType[] types = new DataType[fieldNo.length];
		for (int i=0; i < fieldNo.length; i++) {
			filteredTuple[i] = row.fields[fieldNo[i]];
			types[i] = row.types[fieldNo[i]];
		}
		return new DBTuple(filteredTuple, types);
	}

	@Override
	public void close() {
		child.close();
	}
}
