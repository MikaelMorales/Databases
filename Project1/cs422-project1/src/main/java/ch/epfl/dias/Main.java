package ch.epfl.dias;

import ch.epfl.dias.store.DataType;

public class Main {

	public static void main(String[] args) {

		DataType[] schema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT,
				DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT };

		DataType[] orderSchema = new DataType[] { DataType.INT, DataType.INT, DataType.STRING, DataType.DOUBLE,
				DataType.STRING, DataType.STRING, DataType.STRING, DataType.INT, DataType.STRING };

		schema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT,
				DataType.INT, DataType.INT, DataType.INT, DataType.INT };

/*
		RowStore rowstore = new RowStore(orderSchema, "input/orders_small.csv", "\\|");
		rowstore.load();
		System.out.println(rowstore.getRow(9).toString());


		PAXStore paxstore = new PAXStore(orderSchema, "input/orders_small.csv", "\\|", 3);
		paxstore.load();
		System.out.println(paxstore.getRow(9).toString());
*/
		// ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(paxstore);
		// DBTuple currentTuple = scan.next();
		// while (!currentTuple.eof) {
		// 	System.out.println(currentTuple.getFieldAsString(1));
		// 	currentTuple = scan.next();
		// }

/*
		ColumnStore columnstoreData = new ColumnStore(schema, "input/data.csv", ",");
		columnstoreData.load();
		DBColumn[] db = columnstoreData.getColumns(new int[]{0, 1 , 7});
		for (DBColumn col : db) {
			System.out.println(col.toString());
		}
*/
		//
		// ch.epfl.dias.ops.block.Scan scan = new ch.epfl.dias.ops.block.Scan(columnstoreData);
		// ch.epfl.dias.ops.block.Select sel = new ch.epfl.dias.ops.block.Select(scan, BinaryOp.EQ, 3, 6);
		// ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 2);
		// DBColumn[] result = agg.execute();
		// int output = result[0].getAsInteger()[0];
		// System.out.println(output);
	}
}
