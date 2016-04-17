package edu.wiki.index;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import edu.wiki.util.counting.Counting;

public class SortFile implements Comparator<String>{
	File f;
	File fout;
	int maxMem =  250 * 1024 * 1024; // use 250M in memory
	public SortFile(String fname, String foutname){
		f = new File(fname);
		fout = new File(foutname);
	}
	
	public void sort() throws IOException{
		List<File> tmp = readFile();
		mergeFiles(tmp);
	}

	private boolean fillQueue(LinkedList<String> l, BufferedReader r, int mem) throws IOException{
		if(r == null){
			return true;
		}
		int sz = 0;
		String nl = null;
		while(sz < mem && (nl = r.readLine()) != null){
			l.add(nl);
			sz+=nl.length();
		}
		return nl == null;
	}
	
	private boolean hasMore(List<BufferedReader> readers, List<LinkedList<String>> qs){
		for(BufferedReader r : readers){
			if (r != null){
				return true;
			}
		}
		for(LinkedList<String> q : qs){
			if (!q.isEmpty()){
				return true;
			}
		}
		return false;
	}
	
	private void mergeFiles(List<File> tmp) throws IOException{
		List<BufferedReader> readers = new ArrayList<>();
		List<LinkedList<String>> qs = new ArrayList<>();
		tmp.forEach((f)->{
			try{
				LinkedList<String> q = new LinkedList<>();
				BufferedReader r = new BufferedReader(new FileReader(f));
				boolean finished = fillQueue(q,r, maxMem / tmp.size());
				readers.add(finished ? null : r);
				qs.add(q);
			}catch(Exception e){
				throw new RuntimeException(e);
			}
		});
		
		System.out.println("Merging files...");
		Counting c = new Counting(10);
		FileWriter fw = new FileWriter(fout);
		while(hasMore(readers, qs)){
			String first = null;
			int j = -1;
			
			for(int i = 0; i < qs.size(); i++){
				if(!qs.get(i).isEmpty() && 
						(first == null || compare(first,qs.get(i).peek()) > 0)){
					first = qs.get(i).peek();
					j = i;
				}
			}
			qs.get(j).poll();
			
			if (j != -1 && qs.get(j).isEmpty() && readers.get(j) != null){
				if(fillQueue(qs.get(j), readers.get(j), maxMem / tmp.size())){
					readers.get(j).close();
					readers.set(j, null);
				}
			}

			fw.write(first + "\n");
			c.addOne();
		}
		fw.close();
	}
		
	private List<File> readFile() throws IOException{
		FileInputStream fis = new FileInputStream(f);
		 
		//Construct BufferedReader from InputStreamReader
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));

		System.out.println("Reading unsorted source file:");
		Counting c = new Counting(10);
	 
		List<String> l = new ArrayList<>();
		List<File> tmp = new ArrayList<>();
		int mem = 0;
		String line;
		while ((line = br.readLine()) != null) {
			l.add(line.trim());
			mem+=line.length();
			c.addOne();
			if(mem >= maxMem){
				System.out.println("Writing partial to file...");
				tmp.add(sortIntoFile(l));
				l.clear();
				mem = 0;
			}
		}
		if(!l.isEmpty()){
			System.out.println("Writing partial to file...");
			tmp.add(sortIntoFile(l));
		}
	 
		br.close();
		return tmp;
	}
	@SuppressWarnings("resource")
	private File sortIntoFile(List<String> l) throws IOException{
		Collections.sort(l,this);
		File f = File.createTempFile("myindexer", ".tmp");
		FileWriter w = new FileWriter(f);
		l.forEach((s)->{
			try{
				w.write(s + "\n");
			}catch(Exception e){
				throw new RuntimeException(e);
			}
		});
		w.close();
		return f;
	}

	@Override
	public int compare(String s1, String s2) {
		int i1 = s1.indexOf(' ');
		int i2 = s2.indexOf(' ');
		if (i1 >= 0){
			s1 = s1.substring(0, i1);
		}
		if (i2 >= 0){
			s2 = s2.substring(0, i2);
		}
		return s1.compareToIgnoreCase(s2);
	}

}
