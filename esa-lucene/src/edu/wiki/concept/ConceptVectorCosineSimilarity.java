package edu.wiki.concept;

import edu.wiki.api.concept.IConceptIterator;
import edu.wiki.api.concept.IConceptVector;
import edu.wiki.api.concept.search.IScorer;


public class ConceptVectorSimilarity {

	IScorer m_scorer;
	
	public ConceptVectorSimilarity( IScorer scorer ) {
		m_scorer = scorer;
	}
	
	public double calcSimilarity( IConceptVector v0, IConceptVector v1 ) {
		// Some speed optimization, can speed things up by a lot! 
		if (v0.count() > v1.count()) {
			IConceptVector tmp = v0;
			v0 = v1;
			v1 = tmp;
		}
		
		m_scorer.reset( v0.getData(), v1.getData(), 1 );
		
		IConceptIterator it0 = v0.iterator();
		while( it0.next() ) {
			double value1 = v1.get( it0.getId() );
			if( value1 > 0 ) {
				m_scorer.addConcept( it0.getId(), it0.getValue(), it0.getId(), value1, 1 );
			}
		}
		
		m_scorer.finalizeScore( v0.getData(), v1.getData() );
		
		return m_scorer.getScore();
	}
	
	public double calcSimilarityFast( ArrayListConceptVector v0, ArrayListConceptVector v1 ) {
		m_scorer.reset( v0.getData(), v1.getData(), 1 );
		
		// when dealing with ArrayListConceptVector we are guarenteed the
		// concepts are sorted by id
		IConceptIterator it0 = v0.iterator();
		IConceptIterator it1 = v1.iterator();
		if ((!it0.next())||(!it1.next())) {
			return 0;
		}
		boolean done = false;
		while(!done) {
			if (it0.getId() == it1.getId()) {
				m_scorer.addConcept( it0.getId(), it0.getValue(), it1.getId(), it1.getValue(), 1);
				done = (!it0.next()) || (!it1.next());
			} else {
				done = it0.getId() < it1.getId() ? !it0.next() : !it1.next();
			}
		}
		
		m_scorer.finalizeScore( v0.getData(), v1.getData() );
		
		return m_scorer.getScore();
	}
	
	/**
	 * Assumes vector is normalized!!
	 */
	public double calcCosineDistanceFast( ArrayListConceptVector v0, ArrayListConceptVector v1 ) {
		return Math.abs(Math.acos(calcSimilarityFast(v0, v1)));
	}
	
	public double calcCosineDistance( IConceptVector v0, IConceptVector v1 ) {
		return Math.abs(Math.acos(calcSimilarity(v0, v1)));
	}
}
