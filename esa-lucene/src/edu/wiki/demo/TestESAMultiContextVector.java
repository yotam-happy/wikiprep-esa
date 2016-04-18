package edu.wiki.demo;

import edu.wiki.api.concept.IConceptVector;
import edu.wiki.search.ESAMultiResolutionSearcher;

public class TestESAMultiContextVector extends AbstractTestClass{
	public static void main(String[] args) {
		TestESAMultiContextVector test = new TestESAMultiContextVector();
		test.doMain(args);
	}

	@Override
	public IConceptVector getVector() {
		try {
			ESAMultiResolutionSearcher searcher = new ESAMultiResolutionSearcher();
			//BufferedReader in = new BufferedReader(new InputStreamReader(System.in,"UTF-8"));
			String text = "Artificial intelligence (AI) is the intelligence exhibited by machines or software. It is also"
					+ " the name of the academic field of study which studies how to create computers and computer" 
					+ " software that are capable of intelligent behavior. Major AI researchers and textbooks define"
					+ " this field as the study and design of intelligent agents,[1] in which an intelligent agent"
					+ " is a system that perceives its environment and takes actions that maximize its chances" 
					+ " of success.[2] John McCarthy, who coined the term in 1955,[3] defines it as the science" 
					+ " and engineering of making intelligent machines.[4] AI research is highly technical and" 
					+ " specialized, and is deeply divided into subfields that often fail to communicate with each"
					+ " other.[5] Some of the division is due to social and cultural factors: subfields have grown up"
					+ " around particular institutions and the work of individual researchers. AI research is also"
					+ " divided by several technical issues. Some subfields focus on the solution of specific problems."
					+ " Others focus on one of several possible approaches or on the use of a particular tool or towards"
					+ " the accomplishment of particular applications. The central problems (or goals) of AI research"
					+ " include reasoning, knowledge, planning, learning, natural language processing (communication),"
					+ " perception and the ability to move and manipulate objects.[6] General intelligence is still"
					+ " among the field's long-term goals.[7] Currently popular approaches include statistical methods,"
					+ " computational intelligence and traditional symbolic AI. There are a large number of tools used"
					+ " in AI, including versions of search and mathematical optimization, logic, methods based on"
					+ " probability and economics, and many others. The AI field is interdisciplinary, in which a number"
					+ " of sciences and professions converge, including computer science, mathematics, psychology,"
					+ " linguistics, philosophy and neuroscience, as well as other specialized fields such as artificial"
					+ " psychology. The field was founded on the claim that a central property of humans, human" 
					+ " intelligence—the sapience of Homo sapiens—can be so precisely described that a machine can be"
					+ " made to simulate it.[8] This raises philosophical issues about the nature of the mind and the"
					+ " ethics of creating artificial beings endowed with human-like intelligence, issues which have" 
					+ " been addressed by myth, fiction and philosophy since antiquity.[9] Artificial intelligence has"
					+ " been the subject of tremendous optimism[10] but has also suffered stunning setbacks.[11] Today"
					+ " it has become an essential part of the technology industry, providing the heavy lifting for many"
					+ " of the most challenging problems in computer science.[12]"; //in.readLine();
			IConceptVector c = searcher.getConceptVectorUsingMultiResolution(text, 20, true, true);
			return c;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}	

}
