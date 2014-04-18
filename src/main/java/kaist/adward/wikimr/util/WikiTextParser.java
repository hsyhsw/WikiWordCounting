package kaist.adward.wikimr.util;

import org.eclipse.mylyn.wikitext.core.parser.Attributes;
import org.eclipse.mylyn.wikitext.core.parser.MarkupParser;
import org.eclipse.mylyn.wikitext.core.parser.markup.MarkupLanguage;
import org.eclipse.mylyn.wikitext.mediawiki.core.MediaWikiLanguage;

import java.util.List;

/**
 * Created by adward on 4/11/14.
 */
public class WikiTextParser {

	private static WikiTextParser self = null;
	private MarkupPlainTextParser parserImpl;

	private WikiTextParser() {
		parserImpl = new MarkupPlainTextParser(new MediaWikiLanguage());
	}

	public static WikiTextParser getInstance() {
		if (self == null) {
			self = new WikiTextParser();
		}

		return self;
	}

	public String parsePlainText(String wikiText) {
		wikiText = wikiText.replaceAll("<", "&lt;");
		wikiText = wikiText.replaceAll(">", "&gt;");

		return parserImpl.parseToPlainText(wikiText);
	}

	public List<String> parseLinks(String wikiText) {
		wikiText = wikiText.replaceAll("<", "&lt;");
		wikiText = wikiText.replaceAll(">", "&gt;");

		return parserImpl.parseLinks(wikiText);
	}
}

/**
 * Created by adward on 4/4/14.
 */
class MarkupPlainTextParser extends MarkupParser {

	private List<String> links;

	public MarkupPlainTextParser(MarkupLanguage markupLanguage) {
		super(markupLanguage);
	}

	public String parseToPlainText(String markupContent) {

		String result = null;

		setBuilder(new PlainTextDocumentBuilder());

		parse(markupContent);

		PlainTextDocumentBuilder builder = (PlainTextDocumentBuilder) getBuilder();
		result = builder.getStringBuilder().toString();
		links = builder.getLinks();
		setBuilder(null);

		return result;
	}

	/**
	 * Parse only links in markupContent for faster processing.
	 * <p>
	 * Plain text in markupContent is not available even after parsing using this method.
	 * </p>
	 *
	 * @param markupContent
	 * @return list of linked documents' titles in markupContent
	 */
	public List<String> parseLinks(String markupContent) {

		setBuilder(new PlainTextDocumentBuilder() {
			@Override
			public void characters(String s) {
				// intentionally blank.
			}

			@Override
			public void endDocument() {
				// intentionally blank.
			}

			@Override
			public void link(Attributes attributes, String link, String linkText) {
				addLink(link);
			}
		});

		parse(markupContent);

		PlainTextDocumentBuilder builder = (PlainTextDocumentBuilder) getBuilder();
		links = builder.getLinks();
		setBuilder(null);

		return links;
	}
}