package kaist.adward.wikimr.util;

import org.eclipse.mylyn.wikitext.core.parser.Attributes;
import org.eclipse.mylyn.wikitext.core.parser.DocumentBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


class PlainTextDocumentBuilder extends DocumentBuilder {

	private static final Pattern garbageLinks = Pattern.compile("^\\w+:[\\w\\W\\s]|[^\\x20-\\x7E]");
	private List<String> links;
	private StringBuilder stringBuilder;

	public PlainTextDocumentBuilder() {
		links = new ArrayList<String>();
		stringBuilder = new StringBuilder();
	}

	public List<String> getLinks() {
		return links;
	}

	public StringBuilder getStringBuilder() {
		return stringBuilder;
	}

	public void setStringBuilder(StringBuilder stringBuilder) {
		this.stringBuilder = stringBuilder;
	}

	@Override
	public void beginDocument() {

	}

	@Override
	public void endDocument() {
		String result = cleanText(stringBuilder.toString());
		stringBuilder.setLength(0);
		stringBuilder.append(result);
	}

	@Override
	public void beginBlock(BlockType blockType, Attributes attributes) {

	}

	@Override
	public void endBlock() {

	}

	@Override
	public void beginSpan(SpanType spanType, Attributes attributes) {

	}

	@Override
	public void endSpan() {

	}

	@Override
	public void beginHeading(int i, Attributes attributes) {

	}

	@Override
	public void endHeading() {

	}

	@Override
	public void characters(String s) {
		stringBuilder.append(s);
	}

	@Override
	public void entityReference(String s) {
		String entity;

		if (s.equals("gt")) {
			entity = ">";
		} else if (s.equals("lt")) {
			entity = "<";
		} else if (s.equals("amp")) {
			entity = "&X;";
		} else { // equals, else
			entity = " ";
		}

		stringBuilder.append(entity);
	}

	@Override
	public void image(Attributes attributes, String s) {

	}

	@Override
	public void link(Attributes attributes, String link, String linkText) {
		if (link.startsWith("/wiki/")) {
			stringBuilder.append(cleanText(linkText));

			// list of links is not parsed with default PlainTextDocumentBuilder
//			addLink(link);
		}
	}

	@Override
	public void imageLink(Attributes attributes, Attributes attributes2, String s, String s2) {

	}

	@Override
	public void acronym(String s, String s2) {
	}

	@Override
	public void lineBreak() {

	}

	@Override
	public void charactersUnescaped(String s) {

	}

	protected void addLink(String link) {
		if (link.startsWith("/wiki/")) {
			// revert '_' back to ' ', remove /wiki/
			link = link.substring(6).replaceAll("_", " ");

			// special, foreign links are not allowed
			Matcher matcher = garbageLinks.matcher(link.trim());
			if (!matcher.find()) {
				links.add(link);
			}
		}
	}

	private String cleanText(String result) {
		// remove REDIRECT
		result = result.replaceAll("REDIRECT", " ");

		// removes whole matched foreign characters.
		result = result.replaceAll("([^\\x20-\\x7E]+)", " ");

		// remove ????: from ????:xxxx
		result = result.replaceAll("[a-zA-Z-]+:(\\w*)", "$1 "); // TODO

		result = result.replaceAll("&lt;.+&gt;", " ");

		// remove tags
		result = result.replaceAll("</?.*?>", " ");

		// replace &???;???;... to space
		result = result.replaceAll("&[a-zA-Z;]+;", " ");

		// remove 's
		result = result.replaceAll("'s", " ");

		// remove non-alphanumeric characters and underscore
		result = result.replaceAll("[\\W_]", " ");

		// remove 5+ digits numbers
		result = result.replaceAll("\\d{5,}", " ");

		// remove 1~2 digits numbers
		result = result.replaceAll("\\s\\d{1,2}\\s", " ");

		// remove excessive whitespaces
		result = result.replaceAll("\\s+", " ").trim();

		return result;
	}
}