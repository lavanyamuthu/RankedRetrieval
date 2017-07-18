
public class TestStemmer {
public static void main(String args[]){
	stemIt("establishement");
}
private static void stemIt(String word){
	 char[] w = new char[501];
     Stemmer s = new Stemmer();
	 char arr[] = word.toCharArray();
	 for(int i=0;i<arr.length;)

    {  int ch = arr[i];i++;
       if (Character.isLetter((char) ch))
       {
          int j = 0;
          while(true)
          {  ch = Character.toLowerCase((char) ch);
             w[j] = (char) ch;
             if (j < 500) j++;
             if(i<arr.length){
             ch = arr[i];i++;
             }
             else
             {
            	
                /* to test add(char ch) */
                for (int c = 0; c < j; c++) s.add(w[c]);

                /* or, to test add(char[] w, int j) */
                /* s.add(w, j); */

                s.stem();
                {  String u;

                   /* and now, to test toString() : */
                   u = s.toString();

                   /* to test getResultBuffer(), getResultLength() : */
                   /* u = new String(s.getResultBuffer(), 0, s.getResultLength()); */

                   System.out.print(u);
                }
                break;
             }
          }
       }
       if (ch < 0) break;
    }
}
}
