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
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import gr.iti.mklab.sm.Configuration;

public class SwearItemFilter extends ItemFilter {

	private Set<String> swearwords = new HashSet<String>();
	
	public SwearItemFilter(Configuration configuration) {
		super(configuration);
		
		 List<String> swearWords = Arrays.asList("adultvideo","adultsex","adultxxx","amateursex","amateurpics","anal","analsex","anus","arse","ar5e","ass","assfucker","assfukka","asshole","threesome",
		    		"ballsack","balls","bastard","bitch","biatch","bigtits","blackasses","blowjob","bollock","bollok","boner","boob","boobies","bugger","bum","butt","buttplug","clitoris",
		    		"blondepussy","bukkake","carsex","cock","cocksuck","cocksucker","cocksucking","cockface","cockhead","cockmunch","c0cksucker","clitoris",
		    		"coon","crap","cum","cumshot","cummer","cunt","cuntlick","cuntlicking","damn","dick","dickhead","dlck","dildo","dogsex","dyke","ejaculate","ejaculation","erotica",
		    		"eatpussy","fag","faggot","feck","fellate","fellatio","felching","fingerfuck","fistfuck","fisting","fuck","fuckme","fudgepacker","flange","masterbation",
		    		"gangbang","goddamn","handjob","homo","horny","jerk","jizz","knobend","labia","lmao","lmfao","muff","nigger","nigga","niggah","nipples","porn","penis","pigfucker","piss","poop",
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
			
			List<String> tokens = new ArrayList<String>();
			CharTermAttribute charTermAtt = tokenizer.addAttribute(CharTermAttribute.class);
			tokenizer.reset();
			while (tokenizer.incrementToken()) {
				String token = charTermAtt.toString();
				if(token.contains("http") || token.contains(".") || token.length() <= 1) {
					continue;
				}
				tokens.add(token);
			}
			tokenizer.end();  
			tokenizer.close();
		
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
