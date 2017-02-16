#!/bin/sh
exec scala "$0" "$@"
!#
object HelloWorld {
    def main(args: Array[String]) {
        println("Hello, world! " + args.toList)

        // val lines = scala.io.Source.fromFile("words.txt").mkString
        // println!(lines[1000])

        val lines = scala.io.Source.fromFile("words.txt").getLines
        println("家".charAt(0));
        println(distance("jaa", "naar"));
        val t0 = System.nanoTime()
        // println(lines(1000))
        lines.foreach(line =>  distance("test123", line))

        val t1 = System.nanoTime()
        println("Elapsed time: " + (t1 - t0)/1e6 + "ms")
    }

    def distance(s1: CharSequence, s2: CharSequence) = {
        import scala.annotation.tailrec
        val len_s1 = s1.length();
        // val len_s2 = s2.chars().count();

        // val s1chars_vec = s1.chars().collect::<Vec<char>>();
        // val s2chars_vec = s2.chars().collect::<Vec<char>>();

        // val len_s1 = s1chars_vec.len();
        // val len_s2 = s2chars_vec.len();
        // val column = collection.mutable.ListBuffer.range(0, len_s1+1)

        val column = new Array[Int](len_s1+1) // 'previous' cost array, horizontally
        // val d = new Array[Int](n + 1) // cost array, horizontally

        @tailrec def fillP(i: Int) {
            column(i) = i
            if (i < len_s1) fillP(i + 1)
        }
        fillP(0)

        // val mut column = (0..len_s1+1).collect::<Vec<_>>();

        // s2.view.zipWithIndex.foreach{case (current_char2, x) => {
        for( x <- 0 to s2.length() -1 ) {
            column(0) = x;
            var lastdiag = x;

            // s1.view.zipWithIndex.foreach{case (current_char1, y) => {
            for( y <- 0 to s1.length() -1 ) {

                if (s1.charAt(y) != s2.charAt(x)) lastdiag+=1
                // if (current_char1 != current_char2) lastdiag+=1
                val olddiag = column(y+1);
                column(y+1) = math.min(column(y+1)+1, math.min(column(y)+1, lastdiag));
                lastdiag = olddiag;
            }
        }
        column(len_s1)

        // for (x, current_char2) in s2.chars().enumerate() {
        //     column[0] = x as u32  + 1;
        //     val mut lastdiag = (x as u32) ;
        //     for (y, current_char1) in s1.chars().enumerate() {
        //         // println!("current_char1: {}", current_char1);
        //         // println!("current_char2: {}", current_char2);
        //         if current_char1 != current_char2 {
        //             lastdiag+=1
        //         }
        //         val olddiag = column[y+1];
        //         column[y+1] = cmp::min(column[y+1]+1, cmp::min(column[y]+1, lastdiag));
        //         lastdiag = olddiag;
        //     }
        // }
        // column[len_s1]

    }

    def levenshtein(s: CharSequence, t: CharSequence) = {
        import scala.annotation.tailrec
        def impl(s: CharSequence, t: CharSequence, n: Int, m: Int) = {
            // Inside impl n <= m!
            val p = new Array[Int](n + 1) // 'previous' cost array, horizontally
            val d = new Array[Int](n + 1) // cost array, horizontally

            @tailrec def fillP(i: Int) {
                p(i) = i
                if (i < n) fillP(i + 1)
            }
            fillP(0)

            @tailrec def eachJ(j: Int, t_j: Char, d: Array[Int], p: Array[Int]): Int = {
                d(0) = j
                @tailrec def eachI(i: Int) {
                    val a = d(i - 1) + 1
                    val b = p(i) + 1
                    d(i) = if (a < b) a else {
                        val c = if (s.charAt(i - 1) == t_j) p(i - 1) else p(i - 1) + 1
                        if (b < c) b else c
                    }
                    if (i < n)
                        eachI(i + 1)
                }
                eachI(1)

                if (j < m)
                    eachJ(j + 1, t.charAt(j), p, d)
                else
                    d(n)
            }
            eachJ(1, t.charAt(0), d, p)
        }

        val n = s.length
        val m = t.length
        if (n == 0) m else if (m == 0) n else {
            if (n > m) impl(t, s, m, n) else impl(s, t, n, m)
        }
    }
}
HelloWorld.main(args)