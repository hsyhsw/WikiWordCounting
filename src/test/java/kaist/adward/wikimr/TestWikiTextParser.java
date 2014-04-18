package kaist.adward.wikimr;

import kaist.adward.wikimr.util.WikiTextParser;
import org.junit.After;
import org.junit.Before;

public class TestWikiTextParser {
	WikiTextParser parser;
	String wikiText;

	@Before
	public void setUp() {
		wikiText = "__NOTOC__ '''Eiffel''' may refer to: ==Engineering== * [[Eiffel Tower]] in Paris, designed by Gustave Eiffel in motif as twin to the Eiffel Bridge, re-dubbed Maria Pia Bridge, previously built in Porto, Portugal * [[Maria Pia Bridge]] in Porto, Portugal, designed and build by Gustave Eiffel - Preceding fraternal twin of Eiffel Tower in Paris, France * [[Eiffel Bridge, Ungheni]], Moldova, designed by Gustave Eiffel * [[Eiffel Bridge, Zrenjanin]], Serbia, build by Gustave Eiffel's company in Paris * [[Eiffel (company)]], successor of Gustave Eiffel's engineering company == Family name == * [[Gustave Eiffel]] (1832–1923), engineer and designer of the Eiffel Tower * [[Erika Eiffel]], an American woman who famously \"married\" the Eiffel Tower ==Entertainment== * [[Eiffel 65]], an electronic music group * \"[[Alec Eiffel]]\", a song by the alternative rock band Pixies * [[Eiffel (band)]], a French rock group ==Computing== * [[Eiffel Software]], a software company * [[EiffelStudio]], a development environment for the Eiffel programming language * [[Eiffel (programming language)]], developed by Bertrand Meyer == See also == * [[Eifel]], region of Germany, origin of this surname * [[Jean Effel]] == References == {{Reflist}} {{disambiguation|surname|geo}} {{DEFAULTSORT:Eiffel}} [[Category:German toponyms]] [[Category:German-language surnames]] [[ar:إيفل (توضيح)]] [[bg:Айфел (пояснение)]] [[ca:Eiffel]] [[cs:Eiffel]] [[da:Eiffel]] [[de:Eiffel]] [[es:Eiffel]] [[fa:ایفل]] [[fr:Eiffel]] [[ko:에펠]] [[it:Eiffel]] [[he:אייפל]] [[lv:Eifelis]] [[lt:Eifelis (reikšmės)]] [[hu:Eiffel (egyértelműsítő lap)]] [[nl:Eiffel]] [[ja:エッフェル]] [[pl:Eiffel]] [[pt:Eiffel]] [[ro:Eiffel]] [[ru:Эйфель]] [[fi:Eiffel]] [[th:ไอเฟล]] [[vi:Eiffel]]";
		parser = WikiTextParser.getInstance();
	}

	@After
	public void tearDown() {

	}
}
