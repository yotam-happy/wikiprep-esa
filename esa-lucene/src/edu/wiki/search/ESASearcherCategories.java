package edu.wiki.search;

import java.util.HashSet;
import java.util.Set;

import edu.wiki.api.concept.IConceptVector;
import edu.wiki.concept.TroveConceptVector;
import edu.wiki.util.db.WikiConceptCategoryLinksQueryOptimizer;
import edu.wiki.util.db.WikiSubCategoryLinksQueryOptimizer;
import gnu.trove.THashMap;

public class ESASearcherCategories {
	public IConceptVector getCategoriesVector(IConceptVector concepts){
		IConceptVector v = new TroveConceptVector(30);
		concepts.forEach((concept,score)->{
			IConceptVector t = getCategoriesVector(concept);
			t.multipty(score);
			v.add(t);
		});
		return v;
	}
	
	public IConceptVector getCategoriesVector(int conceptId){
		Set<Integer> cats = WikiConceptCategoryLinksQueryOptimizer.getInstance().
				doQuery(conceptId);
		if(cats == null){
			return new TroveConceptVector(1);
		}
		IConceptVector v = new TroveConceptVector(cats.size());
		cats.forEach((c)->v.add(c, 1));
		
		Set<Integer> addedCats = new HashSet<>(cats);
		while(!addedCats.isEmpty()){
			THashMap<Integer,Set<Integer>> subCats = WikiSubCategoryLinksQueryOptimizer.getInstance().
					doQuery(addedCats);
			addedCats.clear();
			subCats.forEach((c,sub)->{
				sub.forEach((s)->{
					if(!cats.contains(s)){
						addedCats.add(s);
						cats.add(s);
					}
				});
			});
			addedCats.forEach((c)->v.add(c, 1));
		}

		return v;
	}
}
