
fn normalizeText(text:&str) -> String{
    let newStr = text.to_owned();
    {
        let re = Regex::new("*\([fmn\d)]*").unwrap(); // remove (f)(n)(m)(1)...(9)
        newStr = re.replace(newStr, "");
    }
    {
        let re = Regex::new("[\(\)]").unwrap();  // remove braces
        newStr = re.replace(newStr, "");
    }
    {
        let re = Regex::new("[{}'\"“]").unwrap(); // remove ' " {}
        newStr = re.replace(newStr, "");
    }
    {
        let re = Regex::new("\s\s+").unwrap(); // replace tabs, newlines, double spaces with single spaces
        newStr = re.replace(newStr, " ");
    }
    {
        let re = Regex::new("[,.…]").unwrap();  // remove , .
        newStr = re.replace(newStr, "");
    }
    {
        let re = Regex::new("[;・’-]").unwrap(); // remove ;・’-
        newStr = re.replace(newStr, "");
    }

    text = text.toLowerCase()
    return text.trim()

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