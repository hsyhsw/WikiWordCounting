package kaist.adward.wikimr.mappers;

import kaist.adward.wikimr.model.WikiPage;
import kaist.adward.wikimr.util.ExcludedWords;
import kaist.adward.wikimr.util.WikiTextParser;
import kaist.adward.wikimr.util.WikiXmlSAXParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	private Text word = new Text();
	private LongWritable docId;
	private Set<String> excludedWordSet;
	private String[] tokens;

	private WikiPage wikiPage;

	public WikiTextParser wikiTextParser;

	public InvertedIndexMapper() {
		System.out.println("Init InvertedIndexMapper");
	}

	@Override
	protected void setup(Context context) throws IOException {
		excludedWordSet = ExcludedWords.getWordSet();

		wikiTextParser = WikiTextParser.getInstance();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String page = value.toString();
		try {
			wikiPage = WikiXmlSAXParser.parse(page);
		} catch (SAXException e) {
			e.printStackTrace();
		}

		String wikiText = wikiPage.getWikiText();
		long documentId = wikiPage.getDocumentId();
		docId = new LongWritable(documentId);


		context.setStatus("Current docId " + documentId);

		String plainText = null;
		try {
			plainText = wikiTextParser.parsePlainText(wikiText);
		} catch (StackOverflowError e) {
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String filename = fileSplit.getPath().getName();
			System.out.println(filename + ":");
			System.out.println("\tParsing error(stack ovfl): " + documentId + ", " + wikiPage.getTitle());
			return;
		} catch (Exception e) {
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String filename = fileSplit.getPath().getName();
			System.out.println(filename + ":");
			System.out.println("\tParsing error: " + documentId + ", " + wikiPage.getTitle());
			e.printStackTrace(System.out);
			return;
		}


		tokens = plainText.split(" ");

		Set<String> wordSet = new HashSet<String>();
		for (int i = 0; i < tokens.length; ++i) {
			String token = tokens[i].trim().toLowerCase();
			if (!token.equals("") && !excludedWordSet.contains(token)) {
				if (wordSet.add(token)) {
					word.set(token);
					context.write(word, docId);
				}
			}
		}
	}
}
