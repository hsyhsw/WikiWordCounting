package kaist.adward.wikimr;

import kaist.adward.wikimr.util.WikiTextParser;

/**
 * Created by adward on 4/3/14.
 */
public class ArbitraryManualTest {
	public static void main(String[] args) {
//		String result = WikiTextParser.getInstance().parsePlainText("[[templates:asdfashasd 423 a2 1 s 1 d 12 f]]");
//		System.out.println(result);

		String link = "laksdjf:gasdf";
		link = link.replaceAll("^\\w+:", "");
		System.out.println(link);
	}
}
