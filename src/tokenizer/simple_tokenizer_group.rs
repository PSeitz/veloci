use crate::tokenizer::*;


/// This will group consecutive seperator tokens 
/// "ok---nice" -> ["ok", "---", "nice"]
#[derive(Debug)]
pub struct SimpleTokenizerCharsIterateGroupTokens {
    seperators: Vec<char>
}
impl Tokenizer for SimpleTokenizerCharsIterateGroupTokens {
    fn has_tokens(&self, orignal: &str) -> bool {
        SEPERATORS.is_match(orignal) // TODO
    }

    // fn get_tokens<'a, F>(&self, orignal: &'a str, cb_text: &mut F)
    // where
    //     F: FnMut(&'a str, bool),
    // {
    //     let mut last_returned_byte = 0;
    //     let mut last_was_token = false;
    //     for (char_byte_pos, char) in orignal.char_indices() {
    //         if is_default_seperator(char) {
    //             if char_byte_pos == 0 {
    //                 last_was_token = true;
    //             } else if !last_was_token {
    //                 cb_text(&orignal[last_returned_byte..char_byte_pos], false);
    //                 last_was_token = true;
    //                 last_returned_byte = char_byte_pos;
    //             }
    //         } else if last_was_token {
    //             cb_text(&orignal[last_returned_byte..char_byte_pos], true);
    //             last_was_token = false;
    //             last_returned_byte = char_byte_pos;
    //         }
    //     }

    //     if last_returned_byte != orignal.len() {
    //         cb_text(&orignal[last_returned_byte..orignal.len()], last_was_token);
    //     }
    // }

    fn iter<'a>(&'a self, original: &'a str) -> Box<dyn Iterator<Item = (&'a str, bool)> + 'a>
    {
        Box::new(SimpleTokenizerGroupTokenIter::from_str(original, &self.seperators))
    }
}

impl Default for SimpleTokenizerCharsIterateGroupTokens {
    fn default() -> SimpleTokenizerCharsIterateGroupTokens {
        SimpleTokenizerCharsIterateGroupTokens{
            seperators: DEFAULT_SEPERATORS.to_vec()
        }
    }
}



// #[derive(Debug, Clone)]
// pub struct SimpleTokenizerGroupTokenIter<'a> {
//     original: &'a str,
//     last_returned_byte: usize,
//     last_was_token: bool,
//     char_iter: std::str::CharIndices<'a>, // field_id: u8
// }

// impl<'a> SimpleTokenizerGroupTokenIter<'a> {
//     pub fn from_str(text: &'a str) -> SimpleTokenizerGroupTokenIter<'a> {
//         SimpleTokenizerGroupTokenIter {
//             original: text,
//             last_returned_byte: 0,
//             last_was_token: false,
//             char_iter: text.char_indices(),
//         }
//     }
// }

// impl<'a> Iterator for SimpleTokenizerGroupTokenIter<'a> {
//     type Item = (&'a str, bool);

//     #[inline]
//     fn next(&mut self) -> Option<(&'a str, bool)> {
//         while let Some((char_byte_pos, char)) = self.char_iter.next() {
//             if is_default_seperator(char) {
//                 if char_byte_pos == 0 {
//                     self.last_was_token = true;
//                 } else if !self.last_was_token {
//                     let slice = (&self.original[self.last_returned_byte..char_byte_pos], false);
//                     self.last_was_token = true;
//                     self.last_returned_byte = char_byte_pos;
//                     return Some(slice);
//                 }
//             } else if self.last_was_token {
//                 let slice = (&self.original[self.last_returned_byte..char_byte_pos], true);
//                 self.last_was_token = false;
//                 self.last_returned_byte = char_byte_pos;
//                 return Some(slice);
//             }
//         }

//         if self.last_returned_byte != self.original.len() {
//             let slice = (&self.original[self.last_returned_byte..self.original.len()], self.last_was_token);
//             self.last_returned_byte = self.original.len();
//             Some(slice)
//         } else {
//             None
//         }
//     }
// }

#[derive(Debug, Clone)]
pub struct SimpleTokenizerGroupTokenIter<'a, 'b> {
    seperators: &'b [char],
    original: &'a str,
    last_returned_byte: usize,
    last_was_token: bool,
    char_iter: std::str::CharIndices<'a>, // field_id: u8
}

impl<'a, 'b> SimpleTokenizerGroupTokenIter<'a, 'b> {
    pub fn from_str(text: &'a str, seperators: &'b [char]) -> SimpleTokenizerGroupTokenIter<'a, 'b> {
        SimpleTokenizerGroupTokenIter {
            original: text,
            last_returned_byte: 0,
            last_was_token: false,
            char_iter: text.char_indices(),
            seperators,
        }
    }
}

impl<'a, 'b> Iterator for SimpleTokenizerGroupTokenIter<'a, 'b> {
    type Item = (&'a str, bool);

    #[inline]
    fn next(&mut self) -> Option<(&'a str, bool)> {
        while let Some((char_byte_pos, char)) = self.char_iter.next() {
            if self.seperators.contains(&char) {
                if char_byte_pos == 0 {
                    self.last_was_token = true;
                } else if !self.last_was_token {
                    let slice = (&self.original[self.last_returned_byte..char_byte_pos], false);
                    self.last_was_token = true;
                    self.last_returned_byte = char_byte_pos;
                    return Some(slice);
                }
            } else if self.last_was_token {
                let slice = (&self.original[self.last_returned_byte..char_byte_pos], true);
                self.last_was_token = false;
                self.last_returned_byte = char_byte_pos;
                return Some(slice);
            }
        }

        if self.last_returned_byte != self.original.len() {
            let slice = (&self.original[self.last_returned_byte..self.original.len()], self.last_was_token);
            self.last_returned_byte = self.original.len();
            Some(slice)
        } else {
            None
        }
    }
}



