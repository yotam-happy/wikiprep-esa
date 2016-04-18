package edu.wiki.concept;

import edu.wiki.api.concept.IConceptIterator;
import edu.wiki.api.concept.IConceptVector;


public class ConceptVectorCosineSimilarity {

	public static double cosineSimilarity( IConceptVector v0, IConceptVector v1 ) {
		// Some speed optimization, can speed things up by a lot! 
		if (v0.size() > v1.size()) {
			IConceptVector tmp = v0;
			v0 = v1;
			v1 = tmp;
		}

		double d = 0;
		IConceptIterator it0 = v0.iterator();
		while( it0.next() ) {
			double value1 = v1.get( it0.getId() );
			if( value1 > 0 ) {
				d += it0.getValue() * value1;
			}
		}

		d = d / (v0.norm2() * v1.norm2());

		return d;
	}
	
	public static double cosineSimilarityFast( ArrayListConceptVector v0, ArrayListConceptVector v1 ) {
		
		double d = 0, norm1 = 0, norm2 = 0;
		
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
				d += it0.getValue() * it1.getValue();
				norm1 += it0.getValue() * it0.getValue();
				norm2 += it1.getValue() * it1.getValue();
				done = (!it0.next()) || (!it1.next());
			} else {
				if (it0.getId() < it1.getId()){
					norm1 += it0.getValue() * it0.getValue();
					done = !it0.next();
				}else{
					norm2 += it1.getValue() * it1.getValue();
					done = !it1.next();
				}
			}
		}

		d = d / (Math.sqrt(norm1) * Math.sqrt(norm2));
		
		return d;
	}
	
	/**
	 * Assumes vectors are normalized!!
	 */
	public static double cosineDistanceFast( ArrayListConceptVector v0, ArrayListConceptVector v1 ) {
		if(v0 == null || v1 == null){
			throw new NullPointerException();
		}
		return Math.abs(Math.acos(cosineSimilarityFast(v0, v1)));
	}
	
	public static double calcCosineDistance( IConceptVector v0, IConceptVector v1 ) {
		return Math.abs(Math.acos(cosineSimilarity(v0, v1)));
	}
}
