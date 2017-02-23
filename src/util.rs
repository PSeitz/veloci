
use regex::Regex;

use std::borrow::Cow;
pub  fn normalizeText(text:&str) {
    let mut newStr = text.to_owned();

    lazy_static! {
        static ref REGEXES:Vec<(Regex, & 'static str)> = vec![
            (Regex::new(r"*\([fmn\d])*").unwrap(), " "),
            (Regex::new(r"[\(\)]").unwrap(), " "),  // remove braces
            (Regex::new(r#"[{}'"“]"#).unwrap(), ""), // remove ' " {}
            (Regex::new(r"\s\s+").unwrap(), " "), // replace tabs, newlines, double spaces with single spaces
            (Regex::new(r"[,.…]").unwrap(), ""),  // remove , .
            (Regex::new(r"[;・’-]").unwrap(), "") // remove ;・’-
        ];

        // static ref EEEE:Vec<(& 'static str)> = vec![
        //     (" "),
        //     (" "),  // remove braces
        //     ( ""), // remove ' " {}
        //     (""), // replace tabs, newlines, double spaces with single spaces
        //     (""),  // remove , .
        //     ("") // remove ;・’-
        // ];
    }

    // for a in &*EEEE {
    //     newStr = a;
    //     // let seq = seq_arc.clone();
    //     // let restr = variant.to_string();
    //     // let future = thread::spawn(move || variant.find_iter(&seq).count());
    //     // counts.push((restr, future));
    // }

    for ref tupl in &*REGEXES {
        newStr = (tupl.0).replace(&newStr, tupl.1).into_owned();
        // let seq = seq_arc.clone();
        // let restr = variant.to_string();
        // let future = thread::spawn(move || variant.find_iter(&seq).count());
        // counts.push((restr, future));
    }

    // let yoo :Vec<(Regex, & 'static str)> = vec![
    //         (Regex::new(r"*\([fmn\d])*").unwrap(), " "),
    //         (Regex::new(r"[\(\)]").unwrap(), " "),  // remove braces
    //         (Regex::new(r#"[{}'"“]"#).unwrap(), ""), // remove ' " {}
    //         (Regex::new(r"\s\s+").unwrap(), " "), // replace tabs, newlines, double spaces with single spaces
    //         (Regex::new(r"[,.…]").unwrap(), ""),  // remove , .
    //         (Regex::new(r"[;・’-]").unwrap(), "") // remove ;・’-
    //     ];

    // for (regex, b) in yoo {
    //     newStr = regex.replace(&newStr, b).into_owned();
    // }
    // {
    //     lazy_static! {
    //         static ref re: Regex = Regex::new("*\([fmn\d)]*").unwrap();
    //     }
    //     // let re = Regex::new("*\([fmn\d)]*").unwrap(); // remove (f)(n)(m)(1)...(9)
    //     newStr = re.replace(&newStr, "");
    // }
    // {
    //     let re = Regex::new("[\(\)]").unwrap();  // remove braces
    //     newStr = re.replace(&newStr, "");
    // }
    // {
    //     let re = Regex::new("[{}'\"“]").unwrap(); // remove ' " {}
    //     newStr = re.replace(&newStr, "");
    // }
    // {
    //     let re = Regex::new("\s\s+").unwrap(); // replace tabs, newlines, double spaces with single spaces
    //     newStr = re.replace(&newStr, " ");
    // }
    // {
    //     let re = Regex::new("[,.…]").unwrap();  // remove , .
    //     newStr = re.replace(&newStr, "");
    // }
    // {
    //     let re = Regex::new("[;・’-]").unwrap(); // remove ;・’-
    //     newStr = re.replace(&newStr, "");
    // }

    // text = text.toLowerCase()
    // return text.trim()



}
// assert_eq!(re.replace("1078910", ""), " ");

//     text = text.replace(/ *\([^)]*\) */g, ' ') // remove everything in braces
//     text = text.replace(/[{}'"]/g, '') // remove ' " {}
//     text = text.replace(/\s\s+/g, ' ') // replace tabs, newlines, double spaces with single spaces
//     text = text.replace(/[,.]/g, '') // remove , .
//     text = text.replace(/[;・’-]/g, '') // remove ;・’-
//     text = text.toLowerCase()
//     return text.trim()
// }

//     text = text.replace(/ *\([fmn\d)]*\) */g, ' ') // remove (f)(n)(m)(1)...(9)
//     text = text.replace(/[\(\)]/g, ' ') // remove braces
//     text = text.replace(/[{}'"“]/g, '') // remove ' " {}
//     text = text.replace(/\s\s+/g, ' ') // replace tabs, newlines, double spaces with single spaces
//     text = text.replace(/[,.…]/g, '') // remove , .
//     text = text.replace(/[;・’-]/g, '') // remove ;・’-
//     text = text.toLowerCase()
//     return text.trim()
// }