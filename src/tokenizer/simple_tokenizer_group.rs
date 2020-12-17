use crate::tokenizer::*;

/// This will group consecutive seperator tokens
/// "ok---nice" -> ["ok", "---", "nice"]
#[derive(Debug)]
pub struct SimpleTokenizerCharsIterateGroupTokens {
    pub seperators: Vec<char>,
}
impl Tokenizer for SimpleTokenizerCharsIterateGroupTokens {
    fn has_tokens(&self, text: &str) -> bool {
        let mut iter = self.iter(text);
        iter.next();
        iter.next().is_some()
        // SEPERATORS.is_match(orignal) // TODO
    }

    fn iter<'a>(&'a self, original: &'a str) -> Box<dyn Iterator<Item = (&'a str, bool)> + 'a> {
        Box::new(SimpleTokenizerGroupTokenIter::from_str(original, &self.seperators))
    }
}

impl Default for SimpleTokenizerCharsIterateGroupTokens {
    fn default() -> SimpleTokenizerCharsIterateGroupTokens {
        SimpleTokenizerCharsIterateGroupTokens {
            seperators: DEFAULT_SEPERATORS.to_vec(),
        }
    }
}

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
