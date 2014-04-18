package kaist.adward.wikimr.util;

import kaist.adward.wikimr.model.WikiPage;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.IOException;
import java.io.StringReader;
import java.util.Stack;

public class WikiXmlSAXParser {

	private static SAXParser wikiParser;

	static {
		try {
			wikiParser = SAXParserFactory.newInstance().newSAXParser();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		}
	}

	public static WikiPage parse(String line) throws SAXException, IOException {
		if (wikiParser == null)
			throw new SAXException("No SAXParser instance!");

		line = "<page>" + line + "</page>";
		InputSource inputSource = new InputSource(new StringReader(line));

		WikiHandler handler = new WikiHandler();
		wikiParser.reset();
		wikiParser.parse(inputSource, handler);

		return handler.getPage();
	}
}

class WikiHandler extends DefaultHandler {
	private WikiPage currentPage;
	private String currentTag;
	private Stack<String> openTags;

	private StringBuilder currentWikiText;
	private StringBuilder currentTitle;
	private StringBuilder currentId;

	public void startElement(String uri, String name, String qName, Attributes attr) {
		currentTag = qName;
		if (qName.equalsIgnoreCase("page")) {
			currentPage = new WikiPage();
			currentWikiText = new StringBuilder("");
			currentTitle = new StringBuilder("");
			openTags = new Stack<String>();
		} else if (qName.equalsIgnoreCase("id")) {
			currentId = new StringBuilder("");
		}

		openTags.push(currentTag);
	}

	public void characters(char ch[], int start, int length) {
		if (currentTag.equalsIgnoreCase("title")) {
			currentTitle = currentTitle.append(ch, start, length);
		} else if (currentTag.equalsIgnoreCase("text")) {
			currentWikiText = currentWikiText.append(ch, start, length);
		} else if (currentTag.equalsIgnoreCase("id")) {
			currentId = currentId.append(ch, start, length);
		}
	}

	public void endElement(String uri, String name, String qName) {
		openTags.pop();

		if (qName.equalsIgnoreCase("page")) {
			currentPage.setTitle(currentTitle.toString().trim());
			currentPage.setWikiText(currentWikiText.toString().trim());
		} else if (currentTag.equalsIgnoreCase("id")) {
			String parentTag = openTags.peek();

			if (parentTag.equalsIgnoreCase("page")) {
				if (currentPage.getDocumentId() == null) {
					currentPage.setDocumentId(Long.parseLong(currentId.toString().trim()));
				}
			}
		}
	}

	public WikiPage getPage() {
		return currentPage;
	}
}