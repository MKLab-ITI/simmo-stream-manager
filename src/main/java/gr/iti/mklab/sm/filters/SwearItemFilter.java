package gr.iti.mklab.sm.filters;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import gr.iti.mklab.sm.Configuration;

public class SwearItemFilter extends ItemFilter {

	private Set<String> swearwords = new HashSet<String>();
	
	public SwearItemFilter(Configuration configuration) {
		super(configuration);
		
		 List<String> swearWords = Arrays.asList("escort","adultvideo","adultsex","adultxxx","amateursex","amateurpics","anal","analsex","anus","arse","ar5e","ass","assfucker","assfukka","asshole","threesome",
		    		"ballsack","balls","bastard","bitch","biatch","bigtits","blackasses","blowjob","bollock","bollok","boner","boob","boobies","bugger","bum","butt","buttplug","clitoris",
		    		"blondepussy","bukkake","carsex","cock","cocksuck","cocksucker","cocksucking","cockface","cockhead","cockmunch","c0cksucker","clitoris","brazzers",
		    		"coon","crap","cum","cumshot","cummer","cunt","cuntlick","cuntlicking","damn","dick","dickhead","dlck","dildo","dogsex","dyke","ejaculate","ejaculation","erotica",
		    		"eatpussy","fag","faggot","feck","fellate","fellatio","felching","fingerfuck","fistfuck","fisting","fuck","fuckme","fudgepacker","flange","masterbation",
		    		"gangbang","goddamn","handjob","homo","horny","jerk","jizz","knobend","labia","lmao","lmfao","milf","muff","nigger","nigga","niggah","nipples","porn","pornstar","penis","pigfucker","piss","poop",
		    		"peepshow","prick","pube","pussy","pussie","queer","scrotum","sexxx","shemale","shit","sh1t","shitdick","shiting","shitter","squirting","slut","smegma","spunk","tit", "tits", "titfuck","tittywank","tosser",
		    		"teenporn","turd","twat","transsexual","upskirts","vagina","vulva","wank","wanker","whore","wtf","xxx","xxxvideo","xxxpic");

			swearwords.addAll(swearWords);
	}

	@Override
	public boolean accept(gr.iti.mklab.simmo.core.Object post) {
		try {
			String title = post.getTitle();
			String description = post.getDescription();
			if(title == null && description == null) {
				incrementAccepted();
				return true;
			}
			
			StringBuffer strBuffer = new StringBuffer();
			if(title != null) {
				strBuffer.append(title);
			}
			
			if(description != null) {
				strBuffer.append(description);
			}
			
			Reader reader = new StringReader(strBuffer.toString());
			TokenStream tokenizer = new WhitespaceTokenizer(reader);
			TokenStream stream = new LowerCaseFilter(tokenizer);
			
			List<String> tokens = new ArrayList<String>();
			CharTermAttribute charTermAtt = tokenizer.addAttribute(CharTermAttribute.class);
			stream.reset();
			while (stream.incrementToken()) {
				String token = charTermAtt.toString();
				if(token.length() <= 1) {
					continue;
				}
				
				token = token.replaceAll("#", "");
				token = token.replaceAll("\"", "");
				token = token.replaceAll("'", "");
				token = token.replaceAll(".", "");
				
				tokens.add(token);
			}
			stream.end();  
			stream.close();
		
			for(String token : tokens) {
				if(swearwords.contains(token)) {
					incrementDiscarded();
					return false;
				}
			}
		} catch (IOException e) {
			incrementDiscarded();
			return false;
		}
		
		incrementAccepted();
		return true;
	}

	@Override
	public String name() {
		return "SwearItemFilter";
	}

}
