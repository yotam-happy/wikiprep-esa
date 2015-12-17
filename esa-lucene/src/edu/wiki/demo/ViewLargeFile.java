package edu.wiki.demo;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintWriter;

public class ViewLargeFile {
	public static void main(String[] args) throws FileNotFoundException {
		StringBuffer sb = new StringBuffer();
		int c = 0;
		try (BufferedReader br = new BufferedReader(new FileReader(args[0]))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		       sb.append(line).append('\n');
		       c++;
		       if (c >= 10000) {
		    	   break;
		       }
		    }
		    br.close();
		} catch (Exception e) {
			
		}
		
		PrintWriter out = new PrintWriter(args[1]);
		out.print(sb.toString());
		out.close();
	}	
}
