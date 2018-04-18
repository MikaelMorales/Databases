package streamgenerator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class Generate {

	public static void main(String[] args) throws InterruptedException {

		// read from the input file N lines and copy into a new file into output
		// directory

		// input file.
		String inputFile = args[0];
		System.out.println(inputFile);

		// output directory.
		String outputDirectory = args[1];
		System.out.println(outputDirectory);

		// output filename
		String filename = "data";

		// number of tuples per file.
		int numberOfTuplesPerFile = Integer.parseInt(args[2]);
		System.out.println(numberOfTuplesPerFile);

		// current file id.
		int currentFile = 0;

		// This will reference one line at a time
		String line = null;

		try {
			// FileReader reads text files in the default encoding.
			FileReader fileReader = new FileReader(inputFile);

			// Always wrap FileReader in BufferedReader.
			BufferedReader bufferedReader = new BufferedReader(fileReader);

			int curRow = 0;
			ArrayList<String> buffer = new ArrayList<String>();
			while ((line = bufferedReader.readLine()) != null) {
				if (curRow < numberOfTuplesPerFile) {
					buffer.add(line);
					curRow++;
				} else {
					File fout = new File(outputDirectory + "/" + filename + currentFile + ".txt");
					FileOutputStream fos = new FileOutputStream(fout);

					BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

					for (String item : buffer) {
						bw.write(item);
						bw.newLine();
					}
					bw.close();
					buffer.clear();

					currentFile++;
					curRow = 0;
					TimeUnit.SECONDS.sleep(1);
				}
			}
			// Always close files.
			bufferedReader.close();
		} catch (FileNotFoundException ex) {
			System.out.println("Unable to open file '" + inputFile + "'");
		} catch (IOException ex) {
			System.out.println("Error reading file '" + inputFile + "'");
			// Or we could just do this:
			// ex.printStackTrace();
		}
	}
}
